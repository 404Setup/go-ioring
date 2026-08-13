# go-ioring

Go bindings for the [Windows I/O Ring](https://learn.microsoft.com/windows/win32/api/_ioring/) API.

The package exposes both a low-level submission/completion ring and a managed file I/O interface. It is intended for
high-throughput file operations on Windows. It does **not** implement socket accept, connect, send, receive, or poll,
and therefore does not replace the poller used by package `net`.

## Requirements

- Windows build 22000 or later
- Go 1.26.5 or later
- A process that can load the I/O Ring entry points from `kernel32.dll`

Availability still varies by machine. Always call `QueryCapabilities` at runtime before assuming a version, operation,
or queue size is present.

```shell
go go get github.com/404Setup/go-ioring
```

## Architecture

| Type                                  | Role                                                                                       |
|---------------------------------------|--------------------------------------------------------------------------------------------|
| `Ring`                                | Thin wrapper around the native I/O Ring. Callers own memory, handles, and lifetimes.       |
| `Queue`                               | Managed ring for file I/O. Duplicates handles and pins Go buffers until a batch completes. |
| `Batch`                               | Reusable set of read/write/flush requests executed as one synchronous batch.               |
| `RegisteredFile` / `RegisteredBuffer` | Registered resources. `RegisteredFile` implements `io.ReaderAt` and `io.WriterAt`.         |
| `Pool`                                | Schedules work across multiple `Queue`s without creating worker goroutines.                |

A single `Queue` executes batches serially. Use separate queues or a `Pool`
when concurrent submission is required.

## Quick start

### Managed single-file I/O

```go
queue, err := ioring.NewQueue(64)
if err != nil {
return err
}
defer queue.Close()
n, err := queue.ReadAt(file, buf, 0)
if err != nil {
return err
}
_ = n
```

### Registered files with standard adapters

```go
registered, err := queue.RegisterFile(file)
if err != nil {
return err
}
section := io.NewSectionReader(registered, 0, 64<<10)
data, err := io.ReadAll(section)
```

### Batched registered I/O

```go
files, err := queue.RegisterFiles([]*os.File{file})
if err != nil {
return err
}
buffers, err := queue.RegisterBuffers([][]byte{ make([]byte, 64<<10), make([]byte, 64<<10), })
if err != nil {
return err
}
batch := queue.NewBatch(2)
_ = batch.ReadAtRegistered(files[0], buffers[0], 0, 1, ioring.SQENone)
_ = batch.ReadAtRegistered(files[0], buffers[1], 64<<10, 2, ioring.SQENone)
completions := make([]ioring.Completion, batch.Len())
if _, err := batch.Execute(completions); err != nil {
return err
}
```

`Batch.Execute` stores completions in completion order.
`Batch.ExecuteOrdered` stores them in request order.

### Concurrent work with a pool

```go
pool, err := ioring.NewPool(runtime.GOMAXPROCS(0), 64)
if err != nil {
return err
}
defer pool.Close()
if err := pool.Do(func (q *ioring.Queue) error {
_, err := q.ReadAt(file, buf, 0)
return err
}); err != nil {
return err
}
```

`Pool.Do` temporarily acquires one queue. The callback must not close the queue, retain it after returning, or call
methods on the pool.

## Low-level ring

`Ring` maps almost directly onto the Windows API:

```go
ring, err := ioring.New(256)
if err != nil {
return err
}
defer ring.Close()
var pinner runtime.Pinner
pinner.Pin(unsafe.SliceData(buf))
defer pinner.Unpin()
if err := ring.BuildReadFile; err != nil {
return err
}
if _, err := ring.Submit(ioring.WaitAll, ^uint32(0)); err != nil {
return err
}
completion, ok, err := ring.Pop()
```

`Ring` methods are not synchronized. Do not call `Build*`, `Submit`, `Pop`,
`SetCompletionEvent`, or `Close` concurrently on the same ring. Do not copy a
`Ring` after first use.

Supported builder operations include:

- `BuildReadFile`
- `BuildWriteFile`
- `BuildFlushFile`
- `BuildCancel`
- `BuildRegisterFiles`
- `BuildRegisterBuffers`
- `BuildReadFileScatter`
- `BuildWriteFileGather`

## Lifetime rules

These constraints come from the Windows API and are enforced by the package design:

- Buffers, file handles, registration arrays, and scatter/gather segments must remain valid until their completion is
  removed from the completion queue.
- `userData` values are opaque integers. They do not keep Go objects alive.
- `Queue` and `Batch` pin buffers and retain duplicated handles until every request in the batch completes.
- Registered files and buffers become stale after the next
  `RegisterFiles` / `RegisterBuffers` call or after `Close`.
- `Close` on a `Ring` does not cancel in-flight operations. Drain completions before releasing resources used by those
  operations.

## Platform notes

- Source files are Windows-only (`//go:build windows`).
- Architecture-specific calling conventions live in
  `abi_windows_amd64.go`, `abi_windows_arm64.go`, and `abi_windows_386.go`.
- Native entry points are resolved from `kernel32.dll` at init time.
- If the host does not export the I/O Ring API, constructors return
  `ErrUnsupported`.

## Testing

Run the suite on a Windows host that implements I/O Ring:

```powershell
go test ./...
```

Tests skip automatically when the API is unavailable. Coverage includes ABI layouts, raw ring operations, registered
resources, managed batches, pools, completion events, cancel, and scatter/gather.

## License

BSD 3-Clause. See [LICENSE](LICENSE).