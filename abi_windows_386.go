package ioring

import (
	"syscall"
	"unsafe"
)

func split64(v uint64) (uintptr, uintptr) {
	return uintptr(uint32(v)), uintptr(uint32(v >> 32))
}

func callCreate(fn uintptr, version Version, flags CreateFlags, sqSize, cqSize uint32, handle *uintptr) uintptr {
	r, _, _ := syscall.SyscallN(fn, uintptr(version), uintptr(flags.Required), uintptr(flags.Advisory), uintptr(sqSize), uintptr(cqSize), uintptr(unsafe.Pointer(handle)))
	return r
}

func callIsOpSupported(fn, ring uintptr, op OpCode) bool {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(op))
	return r != 0
}

func callCancel(fn, ring uintptr, file HandleRef, operationUserData, userData uintptr) uintptr {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(file.kind), file.value, operationUserData, userData)
	return r
}

func callRead(fn, ring uintptr, file HandleRef, buffer BufferRef, length uint32, offset uint64, userData uintptr, flags SQEFlags) uintptr {
	bufferLow, bufferHigh := split64(buffer.value)
	offsetLow, offsetHigh := split64(offset)
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(file.kind), file.value, uintptr(buffer.kind), bufferLow, bufferHigh, uintptr(length), offsetLow, offsetHigh, userData, uintptr(flags))
	return r
}

func callWrite(fn, ring uintptr, file HandleRef, buffer BufferRef, length uint32, offset uint64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) uintptr {
	bufferLow, bufferHigh := split64(buffer.value)
	offsetLow, offsetHigh := split64(offset)
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(file.kind), file.value, uintptr(buffer.kind), bufferLow, bufferHigh, uintptr(length), offsetLow, offsetHigh, uintptr(writeFlags), userData, uintptr(flags))
	return r
}

func callFlush(fn, ring uintptr, file HandleRef, mode FileFlushMode, userData uintptr, flags SQEFlags) uintptr {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(file.kind), file.value, uintptr(mode), userData, uintptr(flags))
	return r
}

func callReadScatter(fn, ring uintptr, file HandleRef, count uint32, segments *FileSegment, length uint32, offset uint64, userData uintptr, flags SQEFlags) uintptr {
	offsetLow, offsetHigh := split64(offset)
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(file.kind), file.value, uintptr(count), uintptr(unsafe.Pointer(segments)), uintptr(length), offsetLow, offsetHigh, userData, uintptr(flags))
	return r
}

func callWriteGather(fn, ring uintptr, file HandleRef, count uint32, segments *FileSegment, length uint32, offset uint64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) uintptr {
	offsetLow, offsetHigh := split64(offset)
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(file.kind), file.value, uintptr(count), uintptr(unsafe.Pointer(segments)), uintptr(length), offsetLow, offsetHigh, uintptr(writeFlags), userData, uintptr(flags))
	return r
}
