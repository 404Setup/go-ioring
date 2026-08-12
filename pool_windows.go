package ioring

import (
	"errors"
	"os"
	"sync"
	"syscall"
)

// Pool schedules operations across multiple managed Queues. It is useful when
// callers need concurrent submission, because a single Queue executes batches
// serially.
//
// Pool does not create worker goroutines. A caller temporarily acquires a Queue
// while its operation or [Pool.Do] callback runs.
type Pool struct {
	noCopy noCopy

	mu        sync.Mutex
	queues    []*Queue
	available chan *Queue
	active    sync.WaitGroup
	closed    bool
}

// NewPool creates a Pool containing parallelism Queues. Each Queue has entries
// submission entries. Parallelism must be greater than zero.
func NewPool(parallelism int, entries uint32) (*Pool, error) {
	if parallelism <= 0 {
		return nil, syscall.EINVAL
	}
	pool := &Pool{
		queues:    make([]*Queue, 0, parallelism),
		available: make(chan *Queue, parallelism),
	}
	for range parallelism {
		queue, err := NewQueue(entries)
		if err != nil {
			for _, queue := range pool.queues {
				queue.Close()
			}
			return nil, err
		}
		pool.queues = append(pool.queues, queue)
		pool.available <- queue
	}
	return pool, nil
}

// Parallelism reports the number of Queues in pool.
func (pool *Pool) Parallelism() int {
	if pool == nil {
		return 0
	}
	return len(pool.queues)
}

// Do executes function with an exclusively acquired Queue. Function must not
// close or retain queue, use queue after returning, or call methods on pool.
func (pool *Pool) Do(function func(*Queue) error) error {
	if function == nil {
		return syscall.EINVAL
	}
	queue, err := pool.acquire()
	if err != nil {
		return err
	}
	defer pool.release(queue)
	return function(queue)
}

// ReadAt reads len(buffer) bytes from file starting at offset using an
// available Queue. Its return values follow the contract of [os.File.ReadAt].
func (pool *Pool) ReadAt(file *os.File, buffer []byte, offset int64) (int, error) {
	queue, err := pool.acquire()
	if err != nil {
		return 0, err
	}
	defer pool.release(queue)
	return queue.ReadAt(file, buffer, offset)
}

// WriteAt writes buffer to file starting at offset using an available Queue.
// Its return values follow the contract of [os.File.WriteAt].
func (pool *Pool) WriteAt(file *os.File, buffer []byte, offset int64) (int, error) {
	queue, err := pool.acquire()
	if err != nil {
		return 0, err
	}
	defer pool.release(queue)
	return queue.WriteAt(file, buffer, offset)
}

// Sync commits the current contents of file to stable storage using an
// available Queue.
func (pool *Pool) Sync(file *os.File) error {
	queue, err := pool.acquire()
	if err != nil {
		return err
	}
	defer pool.release(queue)
	return queue.Sync(file)
}

// Close waits for active operations and closes every Queue in pool.
func (pool *Pool) Close() error {
	if pool == nil {
		return ErrClosed
	}
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return ErrClosed
	}
	pool.closed = true
	pool.mu.Unlock()

	pool.active.Wait()
	closeErrors := make([]error, 0, len(pool.queues))
	for _, queue := range pool.queues {
		if err := queue.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

func (pool *Pool) acquire() (*Queue, error) {
	if pool == nil {
		return nil, ErrClosed
	}
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}
	pool.active.Add(1)
	pool.mu.Unlock()
	return <-pool.available, nil
}

func (pool *Pool) release(queue *Queue) {
	pool.available <- queue
	pool.active.Done()
}
