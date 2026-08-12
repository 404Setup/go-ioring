package ioring

import "syscall"

type apiTable struct {
	queryCapabilities  uintptr
	isOpSupported      uintptr
	create             uintptr
	getInfo            uintptr
	submit             uintptr
	close              uintptr
	pop                uintptr
	setCompletionEvent uintptr
	cancel             uintptr
	read               uintptr
	registerFiles      uintptr
	registerBuffers    uintptr
	write              uintptr
	flush              uintptr
	readScatter        uintptr
	writeGather        uintptr
}

var ioringAPI = loadAPI()

func loadAPI() apiTable {
	dll := syscall.NewLazyDLL("kernel32.dll")
	find := func(name string) uintptr {
		p := dll.NewProc(name)
		if p.Find() != nil {
			return 0
		}
		return p.Addr()
	}
	return apiTable{
		queryCapabilities:  find("QueryIoRingCapabilities"),
		isOpSupported:      find("IsIoRingOpSupported"),
		create:             find("CreateIoRing"),
		getInfo:            find("GetIoRingInfo"),
		submit:             find("SubmitIoRing"),
		close:              find("CloseIoRing"),
		pop:                find("PopIoRingCompletion"),
		setCompletionEvent: find("SetIoRingCompletionEvent"),
		cancel:             find("BuildIoRingCancelRequest"),
		read:               find("BuildIoRingReadFile"),
		registerFiles:      find("BuildIoRingRegisterFileHandles"),
		registerBuffers:    find("BuildIoRingRegisterBuffers"),
		write:              find("BuildIoRingWriteFile"),
		flush:              find("BuildIoRingFlushFile"),
		readScatter:        find("BuildIoRingReadFileScatter"),
		writeGather:        find("BuildIoRingWriteFileGather"),
	}
}

func apiAvailable() bool {
	return ioringAPI.queryCapabilities != 0 && ioringAPI.create != 0
}
