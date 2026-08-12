package ioring

import (
	"bytes"
	"errors"
	"io"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func testRing(t testing.TB, entries uint32) *Ring {
	t.Helper()
	ring, err := New(entries)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ring.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return ring
}

func TestABILayout(t *testing.T) {
	ptrSize := unsafe.Sizeof(uintptr(0))
	wantHandleRef := uintptr(8)
	wantBufferRef := uintptr(12)
	wantBufferInfo := uintptr(8)
	wantCompletion := uintptr(12)
	if ptrSize == 8 {
		wantHandleRef = 16
		wantBufferRef = 16
		wantBufferInfo = 16
		wantCompletion = 24
	}
	tests := []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"CreateFlags", unsafe.Sizeof(CreateFlags{}), 8},
		{"Capabilities", unsafe.Sizeof(Capabilities{}), 16},
		{"Info", unsafe.Sizeof(Info{}), 20},
		{"HandleRef", unsafe.Sizeof(HandleRef{}), wantHandleRef},
		{"BufferRef", unsafe.Sizeof(BufferRef{}), wantBufferRef},
		{"BufferInfo", unsafe.Sizeof(BufferInfo{}), wantBufferInfo},
		{"FileSegment", unsafe.Sizeof(FileSegment{}), 8},
		{"Completion", unsafe.Sizeof(Completion{}), wantCompletion},
	}
	for _, test := range tests {
		if test.got != test.want {
			t.Errorf("sizeof(%s) = %d, want %d", test.name, test.got, test.want)
		}
	}
}

func TestCompletionErr(t *testing.T) {
	if err := (Completion{ResultCode: SOK}).Err(); err != nil {
		t.Fatalf("successful Completion.Err = %v", err)
	}
	if err := (Completion{ResultCode: ErrorCorrupt}).Err(); err != ErrorCorrupt {
		t.Fatalf("failed Completion.Err = %v, want %v", err, ErrorCorrupt)
	}
}

func TestCapabilitiesAndInfo(t *testing.T) {
	capabilities, err := QueryCapabilities()
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.MaxVersion < Version1 {
		t.Fatalf("MaxVersion = %d", capabilities.MaxVersion)
	}
	ring := testRing(t, 8)
	info, err := ring.Info()
	if err != nil {
		t.Fatal(err)
	}
	if info.Version > capabilities.MaxVersion {
		t.Fatalf("ring version %d exceeds maximum %d", info.Version, capabilities.MaxVersion)
	}
	if info.SubmissionQueueSize < 8 {
		t.Fatalf("submission queue size = %d, want at least 8", info.SubmissionQueueSize)
	}
	if !ring.Supports(OpRead) {
		t.Fatal("read operation is unsupported")
	}
}

func TestReadFile(t *testing.T) {
	const content = "Windows I/O Ring"
	fileName := t.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	ring := testRing(t, 8)
	buffer := make([]byte, len(content))
	var pinner runtime.Pinner
	pinner.Pin(unsafe.SliceData(buffer))
	defer pinner.Unpin()

	if err := ring.BuildReadFile(
		HandleRefFromHandle(file.Fd()),
		BufferRefFromPointer(unsafe.Pointer(unsafe.SliceData(buffer))),
		uint32(len(buffer)), 0, 42, 0,
	); err != nil {
		t.Fatal(err)
	}
	if submitted, err := ring.Submit(WaitAll, ^uint32(0)); err != nil {
		t.Fatal(err)
	} else if submitted != 1 {
		t.Fatalf("submitted %d entries, want 1", submitted)
	}
	completion, ok, err := ring.Pop()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("completion queue is empty")
	}
	if completion.ResultCode.Failed() {
		t.Fatalf("read failed: %v", completion.ResultCode)
	}
	if completion.UserData != 42 || completion.Information != uintptr(len(content)) {
		t.Fatalf("completion = %+v", completion)
	}
	if string(buffer) != content {
		t.Fatalf("read %q, want %q", buffer, content)
	}
}

func TestRegisteredRead(t *testing.T) {
	const content = "registered Windows I/O Ring resources"
	fileName := t.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	ring := testRing(t, 8)
	buffer := make([]byte, len(content))
	var pinner runtime.Pinner
	pinner.Pin(unsafe.SliceData(buffer))
	defer pinner.Unpin()

	handles := []uintptr{file.Fd()}
	buffers := []BufferInfo{{Address: unsafe.Pointer(unsafe.SliceData(buffer)), Length: uint32(len(buffer))}}
	pinner.Pin(unsafe.SliceData(handles))
	pinner.Pin(unsafe.SliceData(buffers))
	if err := ring.BuildRegisterFiles(handles, 1); err != nil {
		t.Fatal(err)
	}
	if err := ring.BuildRegisterBuffers(buffers, 2); err != nil {
		t.Fatal(err)
	}
	if err := ring.BuildReadFile(HandleRefFromIndex(0), BufferRefFromIndex(0, 0), uint32(len(buffer)), 0, 3, 0); err != nil {
		t.Fatal(err)
	}
	if submitted, err := ring.Submit(WaitAll, ^uint32(0)); err != nil {
		t.Fatal(err)
	} else if submitted != 3 {
		t.Fatalf("submitted %d entries, want 3", submitted)
	}
	completions := make([]Completion, 3)
	if n, err := ring.Drain(completions); err != nil {
		t.Fatal(err)
	} else if n != len(completions) {
		t.Fatalf("drained %d completions, want %d", n, len(completions))
	}
	for _, completion := range completions {
		if completion.ResultCode.Failed() {
			t.Fatalf("operation %d failed: %v", completion.UserData, completion.ResultCode)
		}
	}
	if !bytes.Equal(buffer, []byte(content)) {
		t.Fatalf("read %q, want %q", buffer, content)
	}
}

func TestWriteAndFlush(t *testing.T) {
	ring := testRing(t, 8)
	if !ring.Supports(OpWrite) || !ring.Supports(OpFlush) {
		t.Skip("write and flush operations are unavailable")
	}
	fileName := t.TempDir() + `\output`
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	buffer := []byte("write through an I/O Ring")
	var pinner runtime.Pinner
	pinner.Pin(unsafe.SliceData(buffer))
	defer pinner.Unpin()

	fileRef := HandleRefFromHandle(file.Fd())
	if err := ring.BuildWriteFile(fileRef, bufferRef(buffer), uint32(len(buffer)), 0, 0, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := ring.BuildFlushFile(fileRef, FileFlushDefault, 2, SQEDrainPreceding); err != nil {
		t.Fatal(err)
	}
	if submitted, err := ring.Submit(WaitAll, ^uint32(0)); err != nil {
		t.Fatal(err)
	} else if submitted != 2 {
		t.Fatalf("submitted %d entries, want 2", submitted)
	}
	completions := make([]Completion, 2)
	if n, err := ring.Drain(completions); err != nil || n != 2 {
		t.Fatalf("Drain = %d, %v", n, err)
	}
	for _, completion := range completions {
		if completion.ResultCode.Failed() {
			t.Fatalf("operation %d failed: %v", completion.UserData, completion.ResultCode)
		}
	}
	got, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, buffer) {
		t.Fatalf("wrote %q, want %q", got, buffer)
	}
}

func TestScatterGather(t *testing.T) {
	ring := testRing(t, 8)
	if !ring.Supports(OpReadScatter) || !ring.Supports(OpWriteGather) {
		t.Skip("scatter and gather operations are unavailable")
	}
	pageSize := os.Getpagesize()
	firstPage := bytes.Repeat([]byte{'r'}, pageSize)
	fileName := t.TempDir() + `\scatter-gather`
	if err := os.WriteFile(fileName, firstPage, 0600); err != nil {
		t.Fatal(err)
	}
	name, err := syscall.UTF16PtrFromString(fileName)
	if err != nil {
		t.Fatal(err)
	}
	const fileFlagNoBuffering = 0x20000000
	handle, err := syscall.CreateFile(
		name,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		fileFlagNoBuffering|syscall.FILE_FLAG_OVERLAPPED,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(handle)

	readMemory := virtualPage(t, pageSize)
	writeMemory := virtualPage(t, pageSize)
	readBuffer := unsafe.Slice((*byte)(readMemory), pageSize)
	writeBuffer := unsafe.Slice((*byte)(writeMemory), pageSize)
	copy(writeBuffer, bytes.Repeat([]byte{'w'}, pageSize))
	readSegments := []FileSegment{FileSegmentFromPointer(readMemory)}
	writeSegments := []FileSegment{FileSegmentFromPointer(writeMemory)}
	var segmentPinner runtime.Pinner
	segmentPinner.Pin(unsafe.SliceData(readSegments))
	segmentPinner.Pin(unsafe.SliceData(writeSegments))
	defer segmentPinner.Unpin()
	if err := ring.BuildReadFileScatter(
		HandleRefFromHandle(uintptr(handle)),
		readSegments,
		uint32(pageSize), 0, 1, 0,
	); err != nil {
		t.Fatal(err)
	}
	if err := ring.BuildWriteFileGather(
		HandleRefFromHandle(uintptr(handle)),
		writeSegments,
		uint32(pageSize), uint64(pageSize), 0, 2, 0,
	); err != nil {
		t.Fatal(err)
	}
	if submitted, err := ring.Submit(WaitAll, ^uint32(0)); err != nil || submitted != 2 {
		t.Fatalf("Submit = %d, %v", submitted, err)
	}
	completions := make([]Completion, 2)
	if n, err := ring.Drain(completions); err != nil || n != 2 {
		t.Fatalf("Drain = %d, %v", n, err)
	}
	for _, completion := range completions {
		if completion.ResultCode.Failed() {
			t.Fatalf("operation %d failed: %v", completion.UserData, completion.ResultCode)
		}
	}
	if !bytes.Equal(readBuffer, firstPage) {
		t.Fatal("scatter read returned incorrect data")
	}
	written, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if len(written) != pageSize*2 || !bytes.Equal(written[pageSize:], writeBuffer) {
		t.Fatal("gather write returned incorrect data")
	}
}

func TestCompletionEvent(t *testing.T) {
	capabilities, err := QueryCapabilities()
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Features&FeatureSetCompletionEvent == 0 {
		t.Skip("completion events are unavailable")
	}
	ring := testRing(t, 8)
	event := createEvent(t)
	if err := ring.SetCompletionEvent(uintptr(event)); err != nil {
		t.Fatal(err)
	}
	defer ring.SetCompletionEvent(0)

	fileName := t.TempDir() + `\event`
	if err := os.WriteFile(fileName, []byte("event"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	buffer := make([]byte, len("event"))
	var pinner runtime.Pinner
	pinner.Pin(unsafe.SliceData(buffer))
	defer pinner.Unpin()
	if err := ring.BuildReadFile(HandleRefFromHandle(file.Fd()), bufferRef(buffer), uint32(len(buffer)), 0, 1, 0); err != nil {
		t.Fatal(err)
	}
	if submitted, err := ring.Submit(0, 0); err != nil || submitted != 1 {
		t.Fatalf("Submit = %d, %v", submitted, err)
	}
	if result, err := syscall.WaitForSingleObject(event, 5000); err != nil || result != syscall.WAIT_OBJECT_0 {
		t.Fatalf("WaitForSingleObject = %d, %v", result, err)
	}
	completion, ok, err := ring.Pop()
	if err != nil || !ok || completion.ResultCode.Failed() {
		t.Fatalf("Pop = %+v, %v, %v", completion, ok, err)
	}
}

func TestCancelUnknownOperation(t *testing.T) {
	ring := testRing(t, 8)
	if !ring.Supports(OpCancel) {
		t.Skip("cancel operation is unavailable")
	}
	fileName := t.TempDir() + `\cancel`
	if err := os.WriteFile(fileName, nil, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := ring.BuildCancel(HandleRefFromHandle(file.Fd()), 123, 456); err != nil {
		t.Fatal(err)
	}
	if submitted, err := ring.Submit(WaitAll, ^uint32(0)); err != nil || submitted != 1 {
		t.Fatalf("Submit = %d, %v", submitted, err)
	}
	completion, ok, err := ring.Pop()
	if err != nil || !ok {
		t.Fatalf("Pop = %+v, %v, %v", completion, ok, err)
	}
	if completion.UserData != 456 {
		t.Fatalf("completion user data = %d, want 456", completion.UserData)
	}
}

func TestSubmitTimeout(t *testing.T) {
	ring := testRing(t, 8)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	buffer := make([]byte, 1)
	var pinner runtime.Pinner
	pinner.Pin(unsafe.SliceData(buffer))
	defer pinner.Unpin()
	if err := ring.BuildReadFile(HandleRefFromHandle(reader.Fd()), bufferRef(buffer), 1, 0, 1, 0); err != nil {
		t.Fatal(err)
	}
	if submitted, err := ring.Submit(1, 0); err == nil {
		t.Fatalf("Submit = %d, nil; want timeout", submitted)
	} else if code, ok := err.(HRESULT); !ok {
		t.Fatalf("Submit error type = %T, want HRESULT", err)
	} else if code != ErrorWaitTimeout {
		t.Fatalf("Submit error = %#x, want %#x", uint32(code), uint32(ErrorWaitTimeout))
	}
	if _, err := writer.Write([]byte{'x'}); err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Submit(1, 5000); err != nil {
		t.Fatal(err)
	}
	completion, ok, err := ring.Pop()
	if err != nil || !ok || completion.ResultCode.Failed() {
		t.Fatalf("Pop = %+v, %v, %v", completion, ok, err)
	}
}

func virtualPage(t *testing.T, size int) unsafe.Pointer {
	t.Helper()
	dll := syscall.NewLazyDLL("kernel32.dll")
	virtualAlloc := dll.NewProc("VirtualAlloc")
	virtualFree := dll.NewProc("VirtualFree")
	const (
		memCommit     = 0x1000
		memReserve    = 0x2000
		memRelease    = 0x8000
		pageReadWrite = 0x04
	)
	address, _, errno := virtualAlloc.Call(0, uintptr(size), memCommit|memReserve, pageReadWrite)
	if address == 0 {
		t.Fatalf("VirtualAlloc: %v", errno)
	}
	t.Cleanup(func() {
		if ok, _, errno := virtualFree.Call(address, 0, memRelease); ok == 0 {
			t.Errorf("VirtualFree: %v", errno)
		}
	})
	return unsafe.Pointer(address)
}

func createEvent(t *testing.T) syscall.Handle {
	t.Helper()
	dll := syscall.NewLazyDLL("kernel32.dll")
	createEvent := dll.NewProc("CreateEventW")
	handle, _, errno := createEvent.Call(0, 0, 0, 0)
	if handle == 0 {
		t.Fatalf("CreateEventW: %v", errno)
	}
	t.Cleanup(func() { syscall.CloseHandle(syscall.Handle(handle)) })
	return syscall.Handle(handle)
}

func TestManagedBatch(t *testing.T) {
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	const content = "first block|second block"
	fileName := t.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	first := make([]byte, len("first block"))
	second := make([]byte, len("second block"))
	batch := queue.NewBatch(2)
	if err := batch.ReadAt(file, first, 0, 10, 0); err != nil {
		t.Fatal(err)
	}
	if err := batch.ReadAt(file, second, int64(len("first block|")), 20, 0); err != nil {
		t.Fatal(err)
	}
	completions := make([]Completion, 2)
	if n, err := batch.Execute(completions); err != nil || n != 2 {
		t.Fatalf("Execute = %d, %v", n, err)
	}
	if batch.Len() != 0 {
		t.Fatalf("batch length after Execute = %d", batch.Len())
	}
	if string(first) != "first block" || string(second) != "second block" {
		t.Fatalf("read %q and %q", first, second)
	}
	seen := map[uintptr]bool{}
	for _, completion := range completions {
		if completion.ResultCode.Failed() {
			t.Fatalf("operation %d failed: %v", completion.UserData, completion.ResultCode)
		}
		seen[completion.UserData] = true
	}
	if !seen[10] || !seen[20] {
		t.Fatalf("completion user data = %v", seen)
	}
}

func TestManagedBatchWrite(t *testing.T) {
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if queue.supported&(1<<OpWrite) == 0 {
		t.Skip("write operation is unavailable")
	}

	fileName := t.TempDir() + `\output`
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	batch := queue.NewBatch(2)
	if err := batch.WriteAt(file, []byte("second"), 6, 0, 2, 0); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteAt(file, []byte("first "), 0, 0, 1, 0); err != nil {
		t.Fatal(err)
	}
	completions := make([]Completion, 2)
	if n, err := batch.Execute(completions); err != nil || n != 2 {
		t.Fatalf("Execute = %d, %v", n, err)
	}
	got, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first second" {
		t.Fatalf("wrote %q", got)
	}
}

func TestManagedRegisteredBatch(t *testing.T) {
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	const content = "registered managed batch"
	fileName := t.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	files, err := queue.RegisterFiles([]*os.File{file})
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(content))
	buffers, err := queue.RegisterBuffers([][]byte{buffer})
	if err != nil {
		t.Fatal(err)
	}

	batch := queue.NewBatch(1)
	if err := batch.ReadAtRegistered(files[0], buffers[0], 0, 77, 0); err != nil {
		t.Fatal(err)
	}
	completions := make([]Completion, 1)
	if n, err := batch.Execute(completions); err != nil || n != 1 {
		t.Fatalf("Execute = %d, %v", n, err)
	}
	if completions[0].ResultCode.Failed() || completions[0].UserData != 77 {
		t.Fatalf("completion = %+v", completions[0])
	}
	if string(buffer) != content {
		t.Fatalf("read %q, want %q", buffer, content)
	}

	if _, err := queue.RegisterBuffers(nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.ReadAtRegistered(files[0], buffers[0], 0, 88, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Execute(completions); err != syscall.EINVAL {
		t.Fatalf("Execute with stale buffer = %v, want EINVAL", err)
	}
}

func TestQueueConvenience(t *testing.T) {
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if !queue.Supports(OpRead) || queue.MaxBatchSize() < 8 {
		t.Fatalf("queue capabilities: read=%v, max batch=%d", queue.Supports(OpRead), queue.MaxBatchSize())
	}

	const content = "queue convenience"
	fileName := t.TempDir() + `\file`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(fileName, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	buffer := make([]byte, len(content))
	if n, err := queue.ReadAt(file, buffer, 0); err != nil || n != len(buffer) {
		t.Fatalf("ReadAt = %d, %v", n, err)
	}
	if string(buffer) != content {
		t.Fatalf("ReadAt read %q", buffer)
	}
	longBuffer := make([]byte, len(content)+1)
	if n, err := queue.ReadAt(file, longBuffer, 0); err != io.EOF || n != len(content) {
		t.Fatalf("short ReadAt = %d, %v; want %d, EOF", n, err, len(content))
	}
	if n, err := queue.WriteAt(file, []byte("IORing"), 0); err != nil || n != len("IORing") {
		t.Fatalf("WriteAt = %d, %v", n, err)
	}
	if err := queue.Sync(file); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if string(got[:len("IORing")]) != "IORing" {
		t.Fatalf("WriteAt wrote %q", got)
	}
}

func TestRegisteredFileConvenience(t *testing.T) {
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	const content = "registered convenience"
	fileName := t.TempDir() + `\file`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(fileName, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	registeredFile, err := queue.RegisterFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, len(content))
	if n, err := registeredFile.ReadAt(buffer, 0); err != nil || n != len(buffer) {
		t.Fatalf("RegisteredFile.ReadAt = %d, %v", n, err)
	}
	if string(buffer) != content {
		t.Fatalf("RegisteredFile.ReadAt read %q", buffer)
	}

	registeredStorage := make([]byte, len(content))
	registeredBuffer, err := queue.RegisterBuffer(registeredStorage)
	if err != nil {
		t.Fatal(err)
	}
	if registeredBuffer.Len() != len(registeredStorage) {
		t.Fatalf("RegisteredBuffer.Len = %d", registeredBuffer.Len())
	}
	if n, err := registeredFile.ReadAtBuffer(registeredBuffer, 0); err != nil || n != len(registeredStorage) {
		t.Fatalf("ReadAtBuffer = %d, %v", n, err)
	}
	if string(registeredStorage) != content {
		t.Fatalf("ReadAtBuffer read %q", registeredStorage)
	}
	copy(registeredStorage, "registered I/O Ring")
	if n, err := registeredFile.WriteAtBuffer(registeredBuffer, 0); err != nil || n != len(registeredStorage) {
		t.Fatalf("WriteAtBuffer = %d, %v", n, err)
	}
	if err := registeredFile.Sync(); err != nil {
		t.Fatal(err)
	}

	replacement, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if _, err := queue.RegisterFile(replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := registeredFile.ReadAt(buffer, 0); err != syscall.EINVAL {
		t.Fatalf("stale RegisteredFile.ReadAt = %v, want EINVAL", err)
	}
}

func TestBatchExecuteOrdered(t *testing.T) {
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	fileName := t.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte("abcdef"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	buffers := [][]byte{make([]byte, 1), make([]byte, 2), make([]byte, 3)}
	userData := []uintptr{31, 11, 21}
	batch := queue.NewBatch(len(buffers))
	for i := range buffers {
		if err := batch.ReadAt(file, buffers[i], int64(i), userData[i], 0); err != nil {
			t.Fatal(err)
		}
	}
	completions := make([]Completion, len(buffers))
	if n, err := batch.ExecuteOrdered(completions); err != nil || n != len(completions) {
		t.Fatalf("ExecuteOrdered = %d, %v", n, err)
	}
	for i, completion := range completions {
		if err := completion.Err(); err != nil {
			t.Fatalf("completion %d: %v", i, err)
		}
		if completion.UserData != userData[i] || completion.Information != uintptr(len(buffers[i])) {
			t.Fatalf("completion %d = %+v", i, completion)
		}
	}
}

func TestPool(t *testing.T) {
	pool, err := NewPool(2, 8)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			pool.Close()
		}
	}()
	if pool.Parallelism() != 2 {
		t.Fatalf("Parallelism = %d, want 2", pool.Parallelism())
	}

	entered := make(chan *Queue, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			results <- pool.Do(func(queue *Queue) error {
				entered <- queue
				<-release
				return nil
			})
		}()
	}
	queues := make([]*Queue, 0, 2)
	for range 2 {
		select {
		case queue := <-entered:
			queues = append(queues, queue)
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("concurrent Pool.Do calls did not acquire both queues")
		}
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if queues[0] == queues[1] {
		t.Fatal("concurrent Pool.Do calls acquired the same queue")
	}

	const content = "pool scheduling"
	fileName := t.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	buffer := make([]byte, len(content))
	if n, err := pool.ReadAt(file, buffer, 0); err != nil || n != len(buffer) {
		t.Fatalf("Pool.ReadAt = %d, %v", n, err)
	}
	if string(buffer) != content {
		t.Fatalf("Pool.ReadAt read %q", buffer)
	}
	if err := pool.Do(func(queue *Queue) error {
		batch := queue.NewBatch(1)
		if err := batch.ReadAt(file, buffer, 0, 1, 0); err != nil {
			return err
		}
		var completion [1]Completion
		_, err := batch.ExecuteOrdered(completion[:])
		return err
	}); err != nil {
		t.Fatal(err)
	}

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if _, err := pool.ReadAt(file, buffer, 0); err != ErrClosed {
		t.Fatalf("ReadAt after Close = %v, want ErrClosed", err)
	}
}

func BenchmarkOSFileReadAt(b *testing.B) {
	const content = "0123456789abcdef"
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	buffer := make([]byte, len(content))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if n, err := file.ReadAt(buffer, 0); err != nil || n != len(buffer) {
			b.Fatalf("ReadAt = %d, %v", n, err)
		}
	}
}

func BenchmarkRingReadAt(b *testing.B) {
	const content = "0123456789abcdef"
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	ring := testRing(b, 8)
	buffer := make([]byte, len(content))
	var pinner runtime.Pinner
	pinner.Pin(unsafe.SliceData(buffer))
	defer pinner.Unpin()
	fileRef := HandleRefFromHandle(file.Fd())
	bufferRef := BufferRefFromPointer(unsafe.Pointer(unsafe.SliceData(buffer)))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := ring.BuildReadFile(fileRef, bufferRef, uint32(len(buffer)), 0, 0, 0); err != nil {
			b.Fatal(err)
		}
		if _, err := ring.Submit(WaitAll, ^uint32(0)); err != nil {
			b.Fatal(err)
		}
		completion, ok, err := ring.Pop()
		if err != nil || !ok || completion.ResultCode.Failed() {
			b.Fatalf("completion = %+v, %v, %v", completion, ok, err)
		}
	}
}

func BenchmarkQueueReadAt(b *testing.B) {
	const content = "0123456789abcdef"
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		b.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	buffer := make([]byte, len(content))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if n, err := queue.ReadAt(file, buffer, 0); err != nil || n != len(buffer) {
			b.Fatalf("ReadAt = %d, %v", n, err)
		}
	}
}

func BenchmarkRegisteredFileReadAt(b *testing.B) {
	const content = "0123456789abcdef"
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		b.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	registeredFile, err := queue.RegisterFile(file)
	if err != nil {
		b.Fatal(err)
	}
	buffer := make([]byte, len(content))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if n, err := registeredFile.ReadAt(buffer, 0); err != nil || n != len(buffer) {
			b.Fatalf("ReadAt = %d, %v", n, err)
		}
	}
}

func BenchmarkRegisteredFileReadAtBuffer(b *testing.B) {
	const content = "0123456789abcdef"
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	queue, err := NewQueue(8)
	if errors.Is(err, ErrUnsupported) {
		b.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	registeredFile, err := queue.RegisterFile(file)
	if err != nil {
		b.Fatal(err)
	}
	registeredBuffer, err := queue.RegisterBuffer(make([]byte, len(content)))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if n, err := registeredFile.ReadAtBuffer(registeredBuffer, 0); err != nil || n != len(content) {
			b.Fatalf("ReadAtBuffer = %d, %v", n, err)
		}
	}
}

func BenchmarkOSFileReadAt8(b *testing.B) {
	const block = "0123456789abcdef"
	content := bytes.Repeat([]byte(block), 8)
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, content, 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	buffers := make([][]byte, 8)
	for i := range buffers {
		buffers[i] = make([]byte, len(block))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := range buffers {
			if n, err := file.ReadAt(buffers[i], int64(i*len(block))); err != nil || n != len(block) {
				b.Fatalf("ReadAt = %d, %v", n, err)
			}
		}
	}
}

func BenchmarkOSFileReadAtParallel(b *testing.B) {
	const content = "0123456789abcdef"
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		buffer := make([]byte, len(content))
		for parallel.Next() {
			if n, err := file.ReadAt(buffer, 0); err != nil || n != len(buffer) {
				b.Fatalf("ReadAt = %d, %v", n, err)
			}
		}
	})
}

func BenchmarkPoolReadAtParallel(b *testing.B) {
	const content = "0123456789abcdef"
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, []byte(content), 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	pool, err := NewPool(runtime.GOMAXPROCS(0), 8)
	if errors.Is(err, ErrUnsupported) {
		b.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		buffer := make([]byte, len(content))
		for parallel.Next() {
			if n, err := pool.ReadAt(file, buffer, 0); err != nil || n != len(buffer) {
				b.Fatalf("ReadAt = %d, %v", n, err)
			}
		}
	})
}

func BenchmarkManagedBatchRead8(b *testing.B) {
	const block = "0123456789abcdef"
	content := bytes.Repeat([]byte(block), 8)
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, content, 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	queue, err := NewQueue(16)
	if errors.Is(err, ErrUnsupported) {
		b.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	batch := queue.NewBatch(8)
	buffers := make([][]byte, 8)
	for i := range buffers {
		buffers[i] = make([]byte, len(block))
	}
	completions := make([]Completion, 8)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := range buffers {
			if err := batch.ReadAt(file, buffers[i], int64(i*len(block)), uintptr(i), 0); err != nil {
				b.Fatal(err)
			}
		}
		if n, err := batch.Execute(completions); err != nil || n != len(completions) {
			b.Fatalf("Execute = %d, %v", n, err)
		}
	}
}

func BenchmarkManagedRegisteredBatchRead8(b *testing.B) {
	const block = "0123456789abcdef"
	content := bytes.Repeat([]byte(block), 8)
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, content, 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	queue, err := NewQueue(16)
	if errors.Is(err, ErrUnsupported) {
		b.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	files, err := queue.RegisterFiles([]*os.File{file})
	if err != nil {
		b.Fatal(err)
	}
	buffers := make([][]byte, 8)
	for i := range buffers {
		buffers[i] = make([]byte, len(block))
	}
	registeredBuffers, err := queue.RegisterBuffers(buffers)
	if err != nil {
		b.Fatal(err)
	}
	batch := queue.NewBatch(8)
	completions := make([]Completion, 8)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := range registeredBuffers {
			if err := batch.ReadAtRegistered(files[0], registeredBuffers[i], int64(i*len(block)), uintptr(i), 0); err != nil {
				b.Fatal(err)
			}
		}
		if n, err := batch.Execute(completions); err != nil || n != len(completions) {
			b.Fatalf("Execute = %d, %v", n, err)
		}
	}
}

func BenchmarkManagedRegisteredBatchRead8Ordered(b *testing.B) {
	const block = "0123456789abcdef"
	content := bytes.Repeat([]byte(block), 8)
	fileName := b.TempDir() + `\input`
	if err := os.WriteFile(fileName, content, 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(fileName)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	queue, err := NewQueue(16)
	if errors.Is(err, ErrUnsupported) {
		b.Skip("Windows I/O Ring API is unavailable")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	files, err := queue.RegisterFiles([]*os.File{file})
	if err != nil {
		b.Fatal(err)
	}
	buffers := make([][]byte, 8)
	for i := range buffers {
		buffers[i] = make([]byte, len(block))
	}
	registeredBuffers, err := queue.RegisterBuffers(buffers)
	if err != nil {
		b.Fatal(err)
	}
	batch := queue.NewBatch(8)
	completions := make([]Completion, 8)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := range registeredBuffers {
			if err := batch.ReadAtRegistered(files[0], registeredBuffers[i], int64(i*len(block)), uintptr(i), 0); err != nil {
				b.Fatal(err)
			}
		}
		if n, err := batch.ExecuteOrdered(completions); err != nil || n != len(completions) {
			b.Fatalf("ExecuteOrdered = %d, %v", n, err)
		}
	}
}
