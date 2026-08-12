package ioring

import (
	"runtime"
	"syscall"
	"unsafe"
)

// QueryCapabilities reports the I/O Ring capabilities available to the current
// process.
func QueryCapabilities() (Capabilities, error) {
	if ioringAPI.queryCapabilities == 0 {
		return Capabilities{}, ErrUnsupported
	}
	var capabilities Capabilities
	r, _, _ := syscall.SyscallN(ioringAPI.queryCapabilities, uintptr(unsafe.Pointer(&capabilities)))
	return capabilities, hresult(r)
}

// Ring is a Windows I/O submission and completion ring.
//
// Ring's methods do not synchronize with one another. A caller must not invoke
// Build, Submit, Pop, SetCompletionEvent, or Close concurrently on the same Ring.
// A Ring must not be copied after first use.
type Ring struct {
	noCopy noCopy
	handle uintptr
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Create creates a Ring with the exact version, flags, and minimum queue sizes
// requested by the caller.
func Create(version Version, flags CreateFlags, submissionQueueSize, completionQueueSize uint32) (*Ring, error) {
	if !apiAvailable() {
		return nil, ErrUnsupported
	}
	var handle uintptr
	r := callCreate(ioringAPI.create, version, flags, submissionQueueSize, completionQueueSize, &handle)
	if err := hresult(r); err != nil {
		return nil, err
	}
	return &Ring{handle: handle}, nil
}

// New creates a Ring using the newest version available to the process. It
// requests submission entries and enables skipped builder parameter
// checks. Passing zero for entries requests 256 entries.
func New(entries uint32) (*Ring, error) {
	capabilities, err := QueryCapabilities()
	if err != nil {
		return nil, err
	}
	if entries == 0 {
		entries = 256
	}
	if entries > capabilities.MaxSubmissionQueueSize {
		entries = capabilities.MaxSubmissionQueueSize
	}
	return Create(capabilities.MaxVersion, CreateFlags{Advisory: CreateSkipBuilderParameterChecks}, entries, 0)
}

// Info reports the version, flags, and actual queue sizes of r.
func (r *Ring) Info() (Info, error) {
	if r == nil || r.handle == 0 {
		return Info{}, ErrClosed
	}
	var info Info
	hr, _, _ := syscall.SyscallN(ioringAPI.getInfo, r.handle, uintptr(unsafe.Pointer(&info)))
	return info, hresult(hr)
}

// Supports reports whether r supports op.
func (r *Ring) Supports(op OpCode) bool {
	return r != nil && r.handle != 0 && ioringAPI.isOpSupported != 0 &&
		callIsOpSupported(ioringAPI.isOpSupported, r.handle, op)
}

// SetCompletionEvent associates event with r's completion queue. Passing zero
// removes the current event.
func (r *Ring) SetCompletionEvent(event uintptr) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.setCompletionEvent == 0 {
		return ErrUnsupported
	}
	hr, _, _ := syscall.SyscallN(ioringAPI.setCompletionEvent, r.handle, event)
	return hresult(hr)
}

// BuildCancel appends a request to cancel the operation identified by
// operationUserData on file.
func (r *Ring) BuildCancel(file HandleRef, operationUserData, userData uintptr) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.cancel == 0 {
		return ErrUnsupported
	}
	return hresult(callCancel(ioringAPI.cancel, r.handle, file, operationUserData, userData))
}

// BuildReadFile appends a file read to r's submission queue.
func (r *Ring) BuildReadFile(file HandleRef, buffer BufferRef, length uint32, offset uint64, userData uintptr, flags SQEFlags) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.read == 0 {
		return ErrUnsupported
	}
	return hresult(callRead(ioringAPI.read, r.handle, file, buffer, length, offset, userData, flags))
}

// BuildRegisterFiles appends an operation which replaces r's registered files.
// handles must remain valid until the registration operation completes.
func (r *Ring) BuildRegisterFiles(handles []uintptr, userData uintptr) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.registerFiles == 0 {
		return ErrUnsupported
	}
	if uint64(len(handles)) > uint64(^uint32(0)) {
		return syscall.EINVAL
	}
	var p *uintptr
	if len(handles) != 0 {
		p = unsafe.SliceData(handles)
	}
	hr, _, _ := syscall.SyscallN(ioringAPI.registerFiles, r.handle, uintptr(len(handles)), uintptr(unsafe.Pointer(p)), userData)
	runtime.KeepAlive(handles)
	return hresult(hr)
}

// BuildRegisterBuffers appends an operation which replaces r's registered buffers.
// buffers and the memory they describe must remain valid until registration completes.
func (r *Ring) BuildRegisterBuffers(buffers []BufferInfo, userData uintptr) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.registerBuffers == 0 {
		return ErrUnsupported
	}
	if uint64(len(buffers)) > uint64(^uint32(0)) {
		return syscall.EINVAL
	}
	var p *BufferInfo
	if len(buffers) != 0 {
		p = unsafe.SliceData(buffers)
	}
	hr, _, _ := syscall.SyscallN(ioringAPI.registerBuffers, r.handle, uintptr(len(buffers)), uintptr(unsafe.Pointer(p)), userData)
	runtime.KeepAlive(buffers)
	return hresult(hr)
}

// BuildWriteFile appends a file write to r's submission queue.
func (r *Ring) BuildWriteFile(file HandleRef, buffer BufferRef, length uint32, offset uint64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.write == 0 {
		return ErrUnsupported
	}
	return hresult(callWrite(ioringAPI.write, r.handle, file, buffer, length, offset, writeFlags, userData, flags))
}

// BuildFlushFile appends a file flush to r's submission queue.
func (r *Ring) BuildFlushFile(file HandleRef, mode FileFlushMode, userData uintptr, flags SQEFlags) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.flush == 0 {
		return ErrUnsupported
	}
	return hresult(callFlush(ioringAPI.flush, r.handle, file, mode, userData, flags))
}

// BuildReadFileScatter appends a scatter read to r's submission queue.
func (r *Ring) BuildReadFileScatter(file HandleRef, segments []FileSegment, length uint32, offset uint64, userData uintptr, flags SQEFlags) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.readScatter == 0 {
		return ErrUnsupported
	}
	if uint64(len(segments)) > uint64(^uint32(0)) {
		return syscall.EINVAL
	}
	var p *FileSegment
	if len(segments) != 0 {
		p = unsafe.SliceData(segments)
	}
	hr := callReadScatter(ioringAPI.readScatter, r.handle, file, uint32(len(segments)), p, length, offset, userData, flags)
	runtime.KeepAlive(segments)
	return hresult(hr)
}

// BuildWriteFileGather appends a gather write to r's submission queue.
func (r *Ring) BuildWriteFileGather(file HandleRef, segments []FileSegment, length uint32, offset uint64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	if ioringAPI.writeGather == 0 {
		return ErrUnsupported
	}
	if uint64(len(segments)) > uint64(^uint32(0)) {
		return syscall.EINVAL
	}
	var p *FileSegment
	if len(segments) != 0 {
		p = unsafe.SliceData(segments)
	}
	hr := callWriteGather(ioringAPI.writeGather, r.handle, file, uint32(len(segments)), p, length, offset, writeFlags, userData, flags)
	runtime.KeepAlive(segments)
	return hresult(hr)
}

// Submit submits every constructed entry and optionally waits for
// waitOperations completions for at most milliseconds. Passing zero for
// waitOperations performs no wait.
func (r *Ring) Submit(waitOperations, milliseconds uint32) (uint32, error) {
	if r == nil || r.handle == 0 {
		return 0, ErrClosed
	}
	var submitted uint32
	hr, _, _ := syscall.SyscallN(ioringAPI.submit, r.handle, uintptr(waitOperations), uintptr(milliseconds), uintptr(unsafe.Pointer(&submitted)))
	return submitted, hresult(hr)
}

// Pop removes one completion from r. The boolean result is false when the
// completion queue is empty.
func (r *Ring) Pop() (Completion, bool, error) {
	if r == nil || r.handle == 0 {
		return Completion{}, false, ErrClosed
	}
	var completion Completion
	hr, _, _ := syscall.SyscallN(ioringAPI.pop, r.handle, uintptr(unsafe.Pointer(&completion)))
	h := HRESULT(uint32(hr))
	if h == SFalse {
		return Completion{}, false, nil
	}
	if h.Failed() {
		return Completion{}, false, h
	}
	return completion, true, nil
}

// Drain removes up to len(dst) completions without waiting and returns the
// number removed.
func (r *Ring) Drain(dst []Completion) (int, error) {
	for i := range dst {
		completion, ok, err := r.Pop()
		if err != nil {
			return i, err
		}
		if !ok {
			return i, nil
		}
		dst[i] = completion
	}
	return len(dst), nil
}

// Close releases the resources owned by r. Close does not cancel operations
// already in flight; callers must consume their completions before releasing
// any resources used by those operations.
func (r *Ring) Close() error {
	if r == nil || r.handle == 0 {
		return ErrClosed
	}
	hr, _, _ := syscall.SyscallN(ioringAPI.close, r.handle)
	if err := hresult(hr); err != nil {
		return err
	}
	r.handle = 0
	return nil
}
