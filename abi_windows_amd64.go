package ioring

import (
	"runtime"
	"syscall"
	"unsafe"
)

func callCreate(fn uintptr, version Version, flags CreateFlags, sqSize, cqSize uint32, handle *uintptr) uintptr {
	packedFlags := uintptr(flags.Required) | uintptr(flags.Advisory)<<32
	r, _, _ := syscall.SyscallN(fn, uintptr(version), packedFlags, uintptr(sqSize), uintptr(cqSize), uintptr(unsafe.Pointer(handle)))
	return r
}

func callIsOpSupported(fn, ring uintptr, op OpCode) bool {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(op))
	return r != 0
}

// The Windows x64 ABI passes 16-byte, non-vector structures by reference.
func callCancel(fn, ring uintptr, file HandleRef, operationUserData, userData uintptr) uintptr {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(unsafe.Pointer(&file)), operationUserData, userData)
	runtime.KeepAlive(file)
	return r
}

func callRead(fn, ring uintptr, file HandleRef, buffer BufferRef, length uint32, offset uint64, userData uintptr, flags SQEFlags) uintptr {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(unsafe.Pointer(&file)), uintptr(unsafe.Pointer(&buffer)), uintptr(length), uintptr(offset), userData, uintptr(flags))
	runtime.KeepAlive(file)
	runtime.KeepAlive(buffer)
	return r
}

func callWrite(fn, ring uintptr, file HandleRef, buffer BufferRef, length uint32, offset uint64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) uintptr {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(unsafe.Pointer(&file)), uintptr(unsafe.Pointer(&buffer)), uintptr(length), uintptr(offset), uintptr(writeFlags), userData, uintptr(flags))
	runtime.KeepAlive(file)
	runtime.KeepAlive(buffer)
	return r
}

func callFlush(fn, ring uintptr, file HandleRef, mode FileFlushMode, userData uintptr, flags SQEFlags) uintptr {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(unsafe.Pointer(&file)), uintptr(mode), userData, uintptr(flags))
	runtime.KeepAlive(file)
	return r
}

func callReadScatter(fn, ring uintptr, file HandleRef, count uint32, segments *FileSegment, length uint32, offset uint64, userData uintptr, flags SQEFlags) uintptr {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(unsafe.Pointer(&file)), uintptr(count), uintptr(unsafe.Pointer(segments)), uintptr(length), uintptr(offset), userData, uintptr(flags))
	runtime.KeepAlive(file)
	return r
}

func callWriteGather(fn, ring uintptr, file HandleRef, count uint32, segments *FileSegment, length uint32, offset uint64, writeFlags FileWriteFlags, userData uintptr, flags SQEFlags) uintptr {
	r, _, _ := syscall.SyscallN(fn, ring, uintptr(unsafe.Pointer(&file)), uintptr(count), uintptr(unsafe.Pointer(segments)), uintptr(length), uintptr(offset), uintptr(writeFlags), userData, uintptr(flags))
	runtime.KeepAlive(file)
	return r
}
