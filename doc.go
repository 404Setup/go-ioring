//go:build windows

// Package ioring provides access to the Windows I/O Ring API.
//
// I/O rings are available starting with Windows build 22000. Programs must
// still call [QueryCapabilities] at run time because the supported version,
// operations, and queue sizes can vary between systems.
//
// [Ring] is a low-level interface. Its Build methods do not retain Go values,
// pin memory, synchronize concurrent callers, or otherwise manage the lifetime
// of resources used by an operation. Buffers, file handles, registration
// arrays, and segment arrays must remain valid until their completion is
// removed from the completion queue.
//
// Values passed as userData are opaque integers. They do not keep Go objects
// alive; callers must retain any associated object separately until completion.
//
// [Queue] and [Batch] provide a managed interface for common file operations.
// They keep buffers pinned and use duplicated file handles until every request
// in a batch has completed. The managed interface is intentionally synchronous
// at the batch boundary so that it does not require a goroutine or an operating
// system thread per ring.
//
// [RegisteredFile] implements io.ReaderAt and io.WriterAt and can also operate
// directly on a [RegisteredBuffer]. [Pool] schedules concurrent work across
// multiple Queues without creating worker goroutines.
//
// The Windows I/O Ring API does not define socket accept, connect, send,
// receive, or poll operations. This package therefore does not replace the
// network poller used by package net.
package ioring
