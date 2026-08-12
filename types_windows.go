package ioring

import (
	"errors"
	"syscall"
	"unsafe"
)

// ErrUnsupported indicates that the Windows I/O Ring API or a requested
// operation is not available on the current system.
var ErrUnsupported = errors.New("ioring: unsupported by this system")

// ErrClosed indicates that an operation was attempted on a closed Ring or Queue.
var ErrClosed = errors.New("ioring: closed")

// ErrBatchTooLarge indicates that a Batch has more requests than its Queue's
// submission queue can hold.
var ErrBatchTooLarge = errors.New("ioring: batch exceeds submission queue size")

// Version identifies a version of the Windows I/O Ring API.
type Version uint32

const (
	VersionInvalid Version = 0
	Version1       Version = 1
	Version2       Version = 2
	Version3       Version = 300
	Version4       Version = 400
)

// FeatureFlags describe features supported by an I/O Ring implementation.
type FeatureFlags uint32

const (
	FeatureNone               FeatureFlags = 0
	FeatureUserModeEmulation  FeatureFlags = 1
	FeatureSetCompletionEvent FeatureFlags = 2
)

// OpCode identifies an operation supported by an I/O ring.
type OpCode uint32

const (
	OpNop OpCode = iota
	OpRead
	OpRegisterFiles
	OpRegisterBuffers
	OpCancel
	OpWrite
	OpFlush
	OpReadScatter
	OpWriteGather
)

// SQEFlags alter the behavior of a submission queue entry.
type SQEFlags uint32

const (
	SQENone           SQEFlags = 0
	SQEDrainPreceding SQEFlags = 1
)

// CreateRequiredFlags are creation flags which must be understood by the
// operating system. No required flags are currently defined.
type CreateRequiredFlags uint32

const CreateRequiredNone CreateRequiredFlags = 0

// CreateAdvisoryFlags are creation flags which may be ignored by the operating
// system when they are not understood.
type CreateAdvisoryFlags uint32

const (
	CreateAdvisoryNone CreateAdvisoryFlags = 0

	// CreateSkipBuilderParameterChecks asks Windows to omit redundant parameter
	// validation in Build calls. The kernel still validates submitted entries.
	CreateSkipBuilderParameterChecks CreateAdvisoryFlags = 1
)

// CreateFlags configure a Ring.
type CreateFlags struct {
	Required CreateRequiredFlags
	Advisory CreateAdvisoryFlags
}

// Capabilities describe the I/O Ring implementation of the current process.
type Capabilities struct {
	MaxVersion             Version
	MaxSubmissionQueueSize uint32
	MaxCompletionQueueSize uint32
	Features               FeatureFlags
}

// Info describes a Ring.
type Info struct {
	Version             Version
	Flags               CreateFlags
	SubmissionQueueSize uint32
	CompletionQueueSize uint32
}

// RefKind identifies whether a reference contains a raw resource or the index
// of a resource registered with the ring.
type RefKind uint32

const (
	RefRaw RefKind = iota
	RefRegistered
)

// HandleRef refers to either a Windows file handle or a registered file index.
// Its fields are deliberately hidden so invalid ABI layouts cannot be constructed.
type HandleRef struct {
	kind  RefKind
	value uintptr
}

// HandleRefFromHandle returns a reference to a Windows file handle.
func HandleRefFromHandle(handle uintptr) HandleRef {
	return HandleRef{kind: RefRaw, value: handle}
}

// HandleRefFromIndex returns a reference to a registered file handle.
func HandleRefFromIndex(index uint32) HandleRef {
	return HandleRef{kind: RefRegistered, value: uintptr(index)}
}

// BufferRef refers to either a memory address or an offset in a registered buffer.
type BufferRef struct {
	kind  RefKind
	value uint64
}

// BufferRefFromPointer returns a reference to address. The pointed-to memory
// must remain valid until the operation completes.
func BufferRefFromPointer(address unsafe.Pointer) BufferRef {
	return BufferRef{kind: RefRaw, value: uint64(uintptr(address))}
}

// BufferRefFromIndex returns a reference to offset bytes into a registered buffer.
func BufferRefFromIndex(index, offset uint32) BufferRef {
	return BufferRef{kind: RefRegistered, value: uint64(index) | uint64(offset)<<32}
}

// BufferInfo describes a buffer to register with a Ring.
type BufferInfo struct {
	Address unsafe.Pointer
	Length  uint32
}

// FileSegment describes one page-aligned buffer for a scatter or gather operation.
type FileSegment struct {
	address uint64
}

// FileSegmentFromPointer returns a segment referring to address.
func FileSegmentFromPointer(address unsafe.Pointer) FileSegment {
	return FileSegment{address: uint64(uintptr(address))}
}

// FileWriteFlags configure a write operation.
type FileWriteFlags uint32

const (
	FileWriteNone    FileWriteFlags = 0
	FileWriteThrough FileWriteFlags = 1
)

// FileFlushMode selects the data and metadata flushed by a flush operation.
type FileFlushMode uint32

const (
	FileFlushDefault FileFlushMode = iota
	FileFlushData
	FileFlushMinimalMetadata
	FileFlushNoSync
)

// HRESULT is a result code returned by the Windows I/O Ring API.
type HRESULT uint32

const (
	SOK    HRESULT = 0
	SFalse HRESULT = 1

	ErrorRequiredFlagNotSupported HRESULT = 0x80460001
	ErrorSubmissionQueueFull      HRESULT = 0x80460002
	ErrorVersionNotSupported      HRESULT = 0x80460003
	ErrorSubmissionQueueTooBig    HRESULT = 0x80460004
	ErrorCompletionQueueTooBig    HRESULT = 0x80460005
	ErrorSubmitInProgress         HRESULT = 0x80460006
	ErrorCorrupt                  HRESULT = 0x80460007
	ErrorCompletionQueueTooFull   HRESULT = 0x80460008
	ErrorWaitTimeout              HRESULT = 0x800705b4
)

// Failed reports whether h represents a failed HRESULT.
func (h HRESULT) Failed() bool { return int32(h) < 0 }

// Error returns the system message associated with h.
func (h HRESULT) Error() string { return syscall.Errno(h).Error() }

func hresult(r uintptr) error {
	h := HRESULT(uint32(r))
	if h.Failed() {
		return h
	}
	return nil
}

// Completion is an entry removed from a Ring's completion queue.
type Completion struct {
	UserData    uintptr
	ResultCode  HRESULT
	Information uintptr
}

// Err returns the completion's failure result, or nil if the operation
// succeeded.
func (completion Completion) Err() error {
	if completion.ResultCode.Failed() {
		return completion.ResultCode
	}
	return nil
}

// WaitAll can be passed to [Ring.Submit] to wait for every operation submitted
// so far.
const WaitAll = ^uint32(0)
