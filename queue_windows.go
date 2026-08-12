package ioring

import (
	"errors"
	"io"
	"os"
	"runtime"
	"sync"
	"syscall"
	"unsafe"
)

// Queue is a managed I/O ring for executing synchronous batches of file
// operations. Queue duplicates each distinct file handle used by a Batch and
// pins its Go buffers until all operations in that Batch complete.
//
// Queue is safe for concurrent use. Batches submitted to the same Queue are
// executed one at a time. Use separate Queues when concurrent submission is
// required.
type Queue struct {
	noCopy noCopy

	mu        sync.Mutex
	ring      *Ring
	maxBatch  int
	supported uint16
	process   syscall.Handle
	closed    bool

	fileGeneration uint64
	files          []syscall.Handle

	bufferGeneration uint64
	buffers          [][]byte
	bufferPinner     *runtime.Pinner
}

// NewQueue creates a managed Queue. Passing zero for entries requests 256
// submission entries.
func NewQueue(entries uint32) (*Queue, error) {
	ring, err := New(entries)
	if err != nil {
		return nil, err
	}
	info, err := ring.Info()
	if err != nil {
		ring.Close()
		return nil, err
	}
	var supported uint16
	for op := OpCode(0); op <= OpWriteGather; op++ {
		if ring.Supports(op) {
			supported |= 1 << op
		}
	}
	process, err := syscall.GetCurrentProcess()
	if err != nil {
		ring.Close()
		return nil, err
	}
	q := &Queue{
		ring:      ring,
		maxBatch:  int(info.SubmissionQueueSize),
		supported: supported,
		process:   process,
	}
	runtime.SetFinalizer(q, (*Queue).finalize)
	return q, nil
}

// Supports reports whether q supports op.
func (q *Queue) Supports(op OpCode) bool {
	return q != nil && op <= OpWriteGather && q.supported&(1<<op) != 0
}

// MaxBatchSize reports the maximum number of requests accepted by one Batch.
func (q *Queue) MaxBatchSize() int {
	if q == nil {
		return 0
	}
	return q.maxBatch
}

// NewBatch returns an empty, reusable Batch associated with q. capacity is the
// number of requests and distinct files for which space is preallocated.
func (q *Queue) NewBatch(capacity int) *Batch {
	if capacity < 0 {
		capacity = 0
	}
	return &Batch{
		queue:   q,
		ops:     make([]batchOp, 0, capacity),
		handles: make([]batchHandle, 0, capacity),
	}
}

// ReadAt reads len(buffer) bytes from file starting at offset. Its return
// values follow the contract of [os.File.ReadAt].
func (q *Queue) ReadAt(file *os.File, buffer []byte, offset int64) (int, error) {
	if q == nil {
		return 0, ErrClosed
	}
	if file == nil || offset < 0 || uint64(len(buffer)) > uint64(^uint32(0)) {
		return 0, syscall.EINVAL
	}
	completion, err := q.executeOne(batchOp{
		kind: batchRead, file: file, buffer: buffer, offset: uint64(offset),
	})
	return readResult(completion, uint32(len(buffer)), err)
}

// WriteAt writes buffer to file starting at offset. Its return values follow
// the contract of [os.File.WriteAt].
func (q *Queue) WriteAt(file *os.File, buffer []byte, offset int64) (int, error) {
	if q == nil {
		return 0, ErrClosed
	}
	if file == nil || offset < 0 || uint64(len(buffer)) > uint64(^uint32(0)) {
		return 0, syscall.EINVAL
	}
	completion, err := q.executeOne(batchOp{
		kind: batchWrite, file: file, buffer: buffer, offset: uint64(offset),
	})
	return writeResult(completion, uint32(len(buffer)), err)
}

// Sync commits the current contents of file to stable storage.
func (q *Queue) Sync(file *os.File) error {
	if q == nil {
		return ErrClosed
	}
	if file == nil {
		return syscall.EINVAL
	}
	completion, err := q.executeOne(batchOp{kind: batchFlush, file: file})
	if err != nil {
		return err
	}
	return completion.Err()
}

// Close closes q. It waits for an executing Batch to return before closing the
// underlying Ring.
func (q *Queue) Close() error {
	if q == nil {
		return ErrClosed
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	q.closed = true
	err := q.ring.Close()
	q.releaseRegisteredFiles()
	q.releaseRegisteredBuffers()
	runtime.SetFinalizer(q, nil)
	return err
}

func (q *Queue) finalize() {
	q.Close()
}

// RegisteredFile is a file registered with a Queue. It becomes stale when the
// Queue's files are registered again.
type RegisteredFile struct {
	queue      *Queue
	generation uint64
	index      uint32
}

// RegisteredBuffer is a region of a buffer registered with a Queue. It becomes
// stale when the Queue's buffers are registered again.
type RegisteredBuffer struct {
	queue      *Queue
	generation uint64
	index      uint32
	offset     uint32
	length     uint32
}

// ReadAt reads into buffer from file starting at offset. RegisteredFile
// implements io.ReaderAt using the associated Queue.
func (file RegisteredFile) ReadAt(buffer []byte, offset int64) (int, error) {
	if file.queue == nil {
		return 0, ErrClosed
	}
	if offset < 0 || uint64(len(buffer)) > uint64(^uint32(0)) {
		return 0, syscall.EINVAL
	}
	completion, err := file.queue.executeOne(batchOp{
		kind: batchRead, buffer: buffer, offset: uint64(offset),
		registeredFile: file, useRegisteredFile: true,
	})
	return readResult(completion, uint32(len(buffer)), err)
}

// WriteAt writes buffer to file starting at offset. RegisteredFile implements
// io.WriterAt using the associated Queue.
func (file RegisteredFile) WriteAt(buffer []byte, offset int64) (int, error) {
	if file.queue == nil {
		return 0, ErrClosed
	}
	if offset < 0 || uint64(len(buffer)) > uint64(^uint32(0)) {
		return 0, syscall.EINVAL
	}
	completion, err := file.queue.executeOne(batchOp{
		kind: batchWrite, buffer: buffer, offset: uint64(offset),
		registeredFile: file, useRegisteredFile: true,
	})
	return writeResult(completion, uint32(len(buffer)), err)
}

// ReadAtBuffer reads into a registered buffer from file starting at offset.
func (file RegisteredFile) ReadAtBuffer(buffer RegisteredBuffer, offset int64) (int, error) {
	if file.queue == nil {
		return 0, ErrClosed
	}
	if offset < 0 {
		return 0, syscall.EINVAL
	}
	completion, err := file.queue.executeOne(batchOp{
		kind: batchRead, offset: uint64(offset),
		registeredFile: file, registeredBuffer: buffer,
		useRegisteredFile: true, useRegisteredBuffer: true,
	})
	return readResult(completion, buffer.length, err)
}

// WriteAtBuffer writes a registered buffer to file starting at offset.
func (file RegisteredFile) WriteAtBuffer(buffer RegisteredBuffer, offset int64) (int, error) {
	if file.queue == nil {
		return 0, ErrClosed
	}
	if offset < 0 {
		return 0, syscall.EINVAL
	}
	completion, err := file.queue.executeOne(batchOp{
		kind: batchWrite, offset: uint64(offset),
		registeredFile: file, registeredBuffer: buffer,
		useRegisteredFile: true, useRegisteredBuffer: true,
	})
	return writeResult(completion, buffer.length, err)
}

// Sync commits the current contents of file to stable storage.
func (file RegisteredFile) Sync() error {
	if file.queue == nil {
		return ErrClosed
	}
	completion, err := file.queue.executeOne(batchOp{
		kind: batchFlush, registeredFile: file, useRegisteredFile: true,
	})
	if err != nil {
		return err
	}
	return completion.Err()
}

// Len reports the size of buffer in bytes.
func (buffer RegisteredBuffer) Len() int {
	return int(buffer.length)
}

// Slice returns a region of buffer starting at offset and containing length
// bytes.
func (buffer RegisteredBuffer) Slice(offset, length uint32) (RegisteredBuffer, error) {
	if uint64(offset)+uint64(length) > uint64(buffer.length) {
		return RegisteredBuffer{}, syscall.EINVAL
	}
	buffer.offset += offset
	buffer.length = length
	return buffer, nil
}

// RegisterFiles replaces q's registered files. Queue duplicates the handles
// and owns the duplicates until a later call to RegisterFiles or Close.
func (q *Queue) RegisterFiles(files []*os.File) ([]RegisteredFile, error) {
	if q == nil {
		return nil, ErrClosed
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	if q.supported&(1<<OpRegisterFiles) == 0 {
		return nil, ErrUnsupported
	}
	if uint64(len(files)) > uint64(^uint32(0)) {
		return nil, syscall.EINVAL
	}

	handles := make([]syscall.Handle, len(files))
	rawHandles := make([]uintptr, len(files))
	for i, file := range files {
		handle, err := duplicateFile(q.process, file)
		if err != nil {
			closeHandles(handles[:i])
			return nil, err
		}
		handles[i] = handle
		rawHandles[i] = uintptr(handle)
	}
	var registrationPinner runtime.Pinner
	if len(rawHandles) != 0 {
		registrationPinner.Pin(unsafe.SliceData(rawHandles))
		defer registrationPinner.Unpin()
	}
	if err := q.ring.BuildRegisterFiles(rawHandles, 0); err != nil {
		closeHandles(handles)
		q.closeAfterQueueError()
		return nil, err
	}
	completion, err := q.submitOne()
	if err != nil {
		closeHandles(handles)
		return nil, err
	}
	if completion.ResultCode.Failed() {
		closeHandles(handles)
		return nil, completion.ResultCode
	}

	q.releaseRegisteredFiles()
	q.files = handles
	q.fileGeneration++
	registered := make([]RegisteredFile, len(files))
	for i := range registered {
		registered[i] = RegisteredFile{queue: q, generation: q.fileGeneration, index: uint32(i)}
	}
	return registered, nil
}

// RegisterFile replaces q's registered files with file and returns its
// registered reference.
func (q *Queue) RegisterFile(file *os.File) (RegisteredFile, error) {
	files := [...]*os.File{file}
	registered, err := q.RegisterFiles(files[:])
	if err != nil {
		return RegisteredFile{}, err
	}
	return registered[0], nil
}

// RegisterBuffers replaces q's registered buffers. The buffers are pinned and
// retained until a later call to RegisterBuffers or Close.
func (q *Queue) RegisterBuffers(buffers [][]byte) ([]RegisteredBuffer, error) {
	if q == nil {
		return nil, ErrClosed
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	if q.supported&(1<<OpRegisterBuffers) == 0 {
		return nil, ErrUnsupported
	}
	if uint64(len(buffers)) > uint64(^uint32(0)) {
		return nil, syscall.EINVAL
	}

	pinner := new(runtime.Pinner)
	infos := make([]BufferInfo, len(buffers))
	for i, buffer := range buffers {
		if uint64(len(buffer)) > uint64(^uint32(0)) {
			pinner.Unpin()
			return nil, syscall.EINVAL
		}
		if len(buffer) != 0 {
			address := unsafe.SliceData(buffer)
			pinner.Pin(address)
			infos[i].Address = unsafe.Pointer(address)
		}
		infos[i].Length = uint32(len(buffer))
	}
	var registrationPinner runtime.Pinner
	if len(infos) != 0 {
		registrationPinner.Pin(unsafe.SliceData(infos))
		defer registrationPinner.Unpin()
	}
	if err := q.ring.BuildRegisterBuffers(infos, 0); err != nil {
		pinner.Unpin()
		q.closeAfterQueueError()
		return nil, err
	}
	completion, err := q.submitOne()
	if err != nil {
		pinner.Unpin()
		return nil, err
	}
	if completion.ResultCode.Failed() {
		pinner.Unpin()
		return nil, completion.ResultCode
	}

	q.releaseRegisteredBuffers()
	q.bufferPinner = pinner
	q.buffers = append(q.buffers[:0], buffers...)
	q.bufferGeneration++
	registered := make([]RegisteredBuffer, len(buffers))
	for i, buffer := range buffers {
		registered[i] = RegisteredBuffer{
			queue: q, generation: q.bufferGeneration, index: uint32(i), length: uint32(len(buffer)),
		}
	}
	return registered, nil
}

// RegisterBuffer replaces q's registered buffers with buffer and returns its
// registered reference.
func (q *Queue) RegisterBuffer(buffer []byte) (RegisteredBuffer, error) {
	buffers := [...][]byte{buffer}
	registered, err := q.RegisterBuffers(buffers[:])
	if err != nil {
		return RegisteredBuffer{}, err
	}
	return registered[0], nil
}

type batchOpKind uint8

const (
	batchRead batchOpKind = iota
	batchWrite
	batchFlush
)

type batchOp struct {
	kind       batchOpKind
	file       *os.File
	buffer     []byte
	offset     uint64
	userData   uintptr
	sqeFlags   SQEFlags
	writeFlags FileWriteFlags
	flushMode  FileFlushMode

	registeredFile      RegisteredFile
	registeredBuffer    RegisteredBuffer
	useRegisteredFile   bool
	useRegisteredBuffer bool
}

type batchHandle struct {
	file   *os.File
	handle syscall.Handle
}

// Batch is a reusable collection of managed file operations. A Batch must not
// be copied after first use and its methods must not be called concurrently.
type Batch struct {
	noCopy noCopy

	queue   *Queue
	ops     []batchOp
	handles []batchHandle
}

// Len reports the number of requests in b.
func (b *Batch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.ops)
}

// Reset removes all requests from b while retaining its storage for reuse.
func (b *Batch) Reset() {
	if b == nil {
		return
	}
	clear(b.ops)
	b.ops = b.ops[:0]
}

// ReadAt adds a request to read len(buffer) bytes from file at offset.
// userData is copied to the resulting Completion.
func (b *Batch) ReadAt(file *os.File, buffer []byte, offset int64, userData uintptr, flags SQEFlags) error {
	if file == nil || offset < 0 || uint64(len(buffer)) > uint64(^uint32(0)) {
		return syscall.EINVAL
	}
	b.ops = append(b.ops, batchOp{
		kind:     batchRead,
		file:     file,
		buffer:   buffer,
		offset:   uint64(offset),
		userData: userData,
		sqeFlags: flags,
	})
	return nil
}

// WriteAt adds a request to write buffer to file at offset.
// userData is copied to the resulting Completion.
func (b *Batch) WriteAt(file *os.File, buffer []byte, offset int64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) error {
	if file == nil || offset < 0 || uint64(len(buffer)) > uint64(^uint32(0)) {
		return syscall.EINVAL
	}
	b.ops = append(b.ops, batchOp{
		kind:       batchWrite,
		file:       file,
		buffer:     buffer,
		offset:     uint64(offset),
		writeFlags: writeFlags,
		userData:   userData,
		sqeFlags:   flags,
	})
	return nil
}

// Flush adds a request to flush file.
func (b *Batch) Flush(file *os.File, mode FileFlushMode, userData uintptr, flags SQEFlags) error {
	if file == nil {
		return syscall.EINVAL
	}
	b.ops = append(b.ops, batchOp{
		kind:      batchFlush,
		file:      file,
		flushMode: mode,
		userData:  userData,
		sqeFlags:  flags,
	})
	return nil
}

// ReadAtFile adds a read using a RegisteredFile and an unregistered Go buffer.
func (b *Batch) ReadAtFile(file RegisteredFile, buffer []byte, offset int64, userData uintptr, flags SQEFlags) error {
	if offset < 0 || uint64(len(buffer)) > uint64(^uint32(0)) {
		return syscall.EINVAL
	}
	b.ops = append(b.ops, batchOp{
		kind: batchRead, buffer: buffer, offset: uint64(offset), userData: userData, sqeFlags: flags,
		registeredFile: file, useRegisteredFile: true,
	})
	return nil
}

// WriteAtFile adds a write using a RegisteredFile and an unregistered Go buffer.
func (b *Batch) WriteAtFile(file RegisteredFile, buffer []byte, offset int64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) error {
	if offset < 0 || uint64(len(buffer)) > uint64(^uint32(0)) {
		return syscall.EINVAL
	}
	b.ops = append(b.ops, batchOp{
		kind: batchWrite, buffer: buffer, offset: uint64(offset), writeFlags: writeFlags, userData: userData, sqeFlags: flags,
		registeredFile: file, useRegisteredFile: true,
	})
	return nil
}

// ReadAtRegistered adds a read using registered file and buffer resources.
func (b *Batch) ReadAtRegistered(file RegisteredFile, buffer RegisteredBuffer, offset int64, userData uintptr, flags SQEFlags) error {
	if offset < 0 {
		return syscall.EINVAL
	}
	b.ops = append(b.ops, batchOp{
		kind: batchRead, offset: uint64(offset), userData: userData, sqeFlags: flags,
		registeredFile: file, registeredBuffer: buffer, useRegisteredFile: true, useRegisteredBuffer: true,
	})
	return nil
}

// WriteAtRegistered adds a write using registered file and buffer resources.
func (b *Batch) WriteAtRegistered(file RegisteredFile, buffer RegisteredBuffer, offset int64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) error {
	if offset < 0 {
		return syscall.EINVAL
	}
	b.ops = append(b.ops, batchOp{
		kind: batchWrite, offset: uint64(offset), writeFlags: writeFlags, userData: userData, sqeFlags: flags,
		registeredFile: file, registeredBuffer: buffer, useRegisteredFile: true, useRegisteredBuffer: true,
	})
	return nil
}

// FlushRegistered adds a flush using a RegisteredFile.
func (b *Batch) FlushRegistered(file RegisteredFile, mode FileFlushMode, userData uintptr, flags SQEFlags) error {
	b.ops = append(b.ops, batchOp{
		kind: batchFlush, flushMode: mode, userData: userData, sqeFlags: flags,
		registeredFile: file, useRegisteredFile: true,
	})
	return nil
}

// Execute submits every request in b, waits for all of them to complete, and
// stores their completion entries in dst. Execute returns [io.ErrShortBuffer]
// without changing b when len(dst) is smaller than b.Len.
//
// Completion entries are stored in completion order, which is not necessarily
// request order. Execute resets b before returning after submission starts.
func (b *Batch) Execute(dst []Completion) (int, error) {
	return b.execute(dst, false)
}

// ExecuteOrdered is like [Batch.Execute], but stores completion entries in
// request order. It uses each request's userData only for the returned
// Completion and does not require userData values to be unique.
func (b *Batch) ExecuteOrdered(dst []Completion) (int, error) {
	return b.execute(dst, true)
}

func (b *Batch) execute(dst []Completion, ordered bool) (int, error) {
	if b == nil || b.queue == nil {
		return 0, ErrClosed
	}
	if len(dst) < len(b.ops) {
		return 0, io.ErrShortBuffer
	}
	if len(b.ops) == 0 {
		return 0, nil
	}
	q := b.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, ErrClosed
	}
	if len(b.ops) > q.maxBatch {
		return 0, ErrBatchTooLarge
	}

	for _, op := range b.ops {
		var code OpCode
		switch op.kind {
		case batchRead:
			code = OpRead
		case batchWrite:
			code = OpWrite
		case batchFlush:
			code = OpFlush
		}
		if q.supported&(1<<code) == 0 {
			return 0, ErrUnsupported
		}
		if op.useRegisteredFile &&
			(op.registeredFile.queue != q || op.registeredFile.generation != q.fileGeneration || int(op.registeredFile.index) >= len(q.files)) {
			return 0, syscall.EINVAL
		}
		if op.useRegisteredBuffer &&
			(op.registeredBuffer.queue != q || op.registeredBuffer.generation != q.bufferGeneration || int(op.registeredBuffer.index) >= len(q.buffers)) {
			return 0, syscall.EINVAL
		}
	}

	b.handles = b.handles[:0]
	defer b.releaseHandles()
	for _, op := range b.ops {
		if op.useRegisteredFile {
			continue
		}
		if _, err := b.handleFor(op.file); err != nil {
			return 0, err
		}
	}

	var pinner runtime.Pinner
	pinned := false
	for _, op := range b.ops {
		if !op.useRegisteredBuffer && len(op.buffer) != 0 {
			pinned = true
			pinner.Pin(unsafe.SliceData(op.buffer))
		}
	}
	if pinned {
		defer pinner.Unpin()
	}

	built := 0
	for opIndex, op := range b.ops {
		var file HandleRef
		if op.useRegisteredFile {
			file = HandleRefFromIndex(op.registeredFile.index)
		} else {
			file = HandleRefFromHandle(uintptr(b.duplicatedHandle(op.file)))
		}
		var buffer BufferRef
		var length uint32
		if op.useRegisteredBuffer {
			buffer = BufferRefFromIndex(op.registeredBuffer.index, op.registeredBuffer.offset)
			length = op.registeredBuffer.length
		} else {
			buffer = bufferRef(op.buffer)
			length = uint32(len(op.buffer))
		}
		var err error
		userData := op.userData
		if ordered {
			userData = uintptr(opIndex)
		}
		switch op.kind {
		case batchRead:
			err = q.ring.BuildReadFile(file, buffer, length, op.offset, userData, op.sqeFlags)
		case batchWrite:
			err = q.ring.BuildWriteFile(file, buffer, length, op.offset, op.writeFlags, userData, op.sqeFlags)
		case batchFlush:
			err = q.ring.BuildFlushFile(file, op.flushMode, userData, op.sqeFlags)
		}
		if err != nil {
			q.closeAfterQueueError()
			b.Reset()
			return 0, err
		}
		built++
	}

	submitted, err := q.ring.Submit(WaitAll, ^uint32(0))
	if err != nil {
		q.closeAfterQueueError()
		b.Reset()
		return 0, err
	}
	if submitted != uint32(built) {
		q.closeAfterQueueError()
		b.Reset()
		return 0, errors.New("ioring: incomplete successful submission")
	}

	for i := 0; i < built; i++ {
		completion, ok, err := q.ring.Pop()
		if err != nil {
			q.closeAfterQueueError()
			b.Reset()
			return i, err
		}
		if !ok {
			q.closeAfterQueueError()
			b.Reset()
			return i, errors.New("ioring: missing completion after successful wait")
		}
		if ordered {
			if completion.UserData >= uintptr(built) {
				q.closeAfterQueueError()
				b.Reset()
				return i, errors.New("ioring: invalid ordered completion index")
			}
			requestIndex := int(completion.UserData)
			completion.UserData = b.ops[requestIndex].userData
			dst[requestIndex] = completion
		} else {
			dst[i] = completion
		}
	}
	b.Reset()
	return built, nil
}

func bufferRef(buffer []byte) BufferRef {
	if len(buffer) == 0 {
		return BufferRefFromPointer(nil)
	}
	return BufferRefFromPointer(unsafe.Pointer(unsafe.SliceData(buffer)))
}

func (q *Queue) executeOne(op batchOp) (Completion, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Completion{}, ErrClosed
	}
	var code OpCode
	switch op.kind {
	case batchRead:
		code = OpRead
	case batchWrite:
		code = OpWrite
	case batchFlush:
		code = OpFlush
	}
	if q.supported&(1<<code) == 0 {
		return Completion{}, ErrUnsupported
	}
	if op.useRegisteredFile &&
		(op.registeredFile.queue != q || op.registeredFile.generation != q.fileGeneration || int(op.registeredFile.index) >= len(q.files)) {
		return Completion{}, syscall.EINVAL
	}
	if op.useRegisteredBuffer &&
		(op.registeredBuffer.queue != q || op.registeredBuffer.generation != q.bufferGeneration || int(op.registeredBuffer.index) >= len(q.buffers)) {
		return Completion{}, syscall.EINVAL
	}

	var file HandleRef
	if op.useRegisteredFile {
		file = HandleRefFromIndex(op.registeredFile.index)
	} else {
		handle, err := duplicateFile(q.process, op.file)
		if err != nil {
			return Completion{}, err
		}
		defer syscall.CloseHandle(handle)
		file = HandleRefFromHandle(uintptr(handle))
	}
	var pinner runtime.Pinner
	if !op.useRegisteredBuffer && len(op.buffer) != 0 {
		pinner.Pin(unsafe.SliceData(op.buffer))
		defer pinner.Unpin()
	}
	var buffer BufferRef
	var length uint32
	if op.useRegisteredBuffer {
		buffer = BufferRefFromIndex(op.registeredBuffer.index, op.registeredBuffer.offset)
		length = op.registeredBuffer.length
	} else {
		buffer = bufferRef(op.buffer)
		length = uint32(len(op.buffer))
	}

	var err error
	switch op.kind {
	case batchRead:
		err = q.ring.BuildReadFile(file, buffer, length, op.offset, 0, op.sqeFlags)
	case batchWrite:
		err = q.ring.BuildWriteFile(file, buffer, length, op.offset, op.writeFlags, 0, op.sqeFlags)
	case batchFlush:
		err = q.ring.BuildFlushFile(file, op.flushMode, 0, op.sqeFlags)
	}
	if err != nil {
		q.closeAfterQueueError()
		return Completion{}, err
	}
	return q.submitOne()
}

const (
	hresultHandleEOF       HRESULT = 0x80070026
	hresultStatusEndOfFile HRESULT = 0xd0000011
)

func readResult(completion Completion, requested uint32, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	n := completionBytes(completion, requested)
	if completion.ResultCode == hresultHandleEOF || completion.ResultCode == hresultStatusEndOfFile {
		return n, io.EOF
	}
	if err := completion.Err(); err != nil {
		return n, err
	}
	if uint32(n) != requested {
		return n, io.EOF
	}
	return n, nil
}

func writeResult(completion Completion, requested uint32, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	n := completionBytes(completion, requested)
	if err := completion.Err(); err != nil {
		return n, err
	}
	if uint32(n) != requested {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func completionBytes(completion Completion, requested uint32) int {
	if completion.Information > uintptr(requested) {
		return int(requested)
	}
	return int(completion.Information)
}

func (q *Queue) closeAfterQueueError() {
	q.closed = true
	q.ring.Close()
	q.releaseRegisteredFiles()
	q.releaseRegisteredBuffers()
	runtime.SetFinalizer(q, nil)
}

func (b *Batch) handleFor(file *os.File) (syscall.Handle, error) {
	if handle := b.duplicatedHandle(file); handle != syscall.InvalidHandle {
		return handle, nil
	}
	duplicate, err := duplicateFile(b.queue.process, file)
	if err != nil {
		return syscall.InvalidHandle, err
	}
	b.handles = append(b.handles, batchHandle{file: file, handle: duplicate})
	return duplicate, nil
}

func (b *Batch) duplicatedHandle(file *os.File) syscall.Handle {
	for i := range b.handles {
		if b.handles[i].file == file {
			return b.handles[i].handle
		}
	}
	return syscall.InvalidHandle
}

func (b *Batch) releaseHandles() {
	for i := range b.handles {
		syscall.CloseHandle(b.handles[i].handle)
		b.handles[i] = batchHandle{}
	}
	b.handles = b.handles[:0]
}

func (q *Queue) submitOne() (Completion, error) {
	submitted, err := q.ring.Submit(WaitAll, ^uint32(0))
	if err != nil {
		q.closeAfterQueueError()
		return Completion{}, err
	}
	if submitted != 1 {
		q.closeAfterQueueError()
		return Completion{}, errors.New("ioring: incomplete successful submission")
	}
	completion, ok, err := q.ring.Pop()
	if err != nil {
		q.closeAfterQueueError()
		return Completion{}, err
	}
	if !ok {
		q.closeAfterQueueError()
		return Completion{}, errors.New("ioring: missing completion after successful wait")
	}
	return completion, nil
}

func duplicateFile(process syscall.Handle, file *os.File) (syscall.Handle, error) {
	if file == nil {
		return syscall.InvalidHandle, syscall.EINVAL
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return syscall.InvalidHandle, err
	}
	var duplicate = syscall.InvalidHandle
	var duplicateErr error
	err = raw.Control(func(handle uintptr) {
		duplicateErr = syscall.DuplicateHandle(
			process,
			syscall.Handle(handle),
			process,
			&duplicate,
			0,
			false,
			syscall.DUPLICATE_SAME_ACCESS,
		)
	})
	if err != nil {
		return syscall.InvalidHandle, err
	}
	if duplicateErr != nil {
		return syscall.InvalidHandle, duplicateErr
	}
	return duplicate, nil
}

func closeHandles(handles []syscall.Handle) {
	for _, handle := range handles {
		if handle != syscall.InvalidHandle {
			syscall.CloseHandle(handle)
		}
	}
}

func (q *Queue) releaseRegisteredFiles() {
	closeHandles(q.files)
	clear(q.files)
	q.files = nil
}

func (q *Queue) releaseRegisteredBuffers() {
	if q.bufferPinner != nil {
		q.bufferPinner.Unpin()
		q.bufferPinner = nil
	}
	clear(q.buffers)
	q.buffers = nil
}
