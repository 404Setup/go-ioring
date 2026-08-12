package ioring_test

import (
	"io"
	"log"
	"os"

	"github.com/404Setup/go-ioring"
)

func ExampleRegisteredFile() {
	file, err := os.Open("data.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	queue, err := ioring.NewQueue(64)
	if err != nil {
		log.Fatal(err)
	}
	defer queue.Close()
	registered, err := queue.RegisterFile(file)
	if err != nil {
		log.Fatal(err)
	}

	// RegisteredFile implements io.ReaderAt, so standard adapters work without
	// any I/O Ring-specific glue.
	section := io.NewSectionReader(registered, 0, 64<<10)
	data, err := io.ReadAll(section)
	if err != nil {
		log.Fatal(err)
	}
	_ = data
}

func ExampleBatch() {
	file, err := os.Open("data.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	queue, err := ioring.NewQueue(64)
	if err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	files, err := queue.RegisterFiles([]*os.File{file})
	if err != nil {
		log.Fatal(err)
	}
	buffers, err := queue.RegisterBuffers([][]byte{
		make([]byte, 64<<10),
		make([]byte, 64<<10),
	})
	if err != nil {
		log.Fatal(err)
	}

	batch := queue.NewBatch(2)
	if err := batch.ReadAtRegistered(files[0], buffers[0], 0, 1, ioring.SQENone); err != nil {
		log.Fatal(err)
	}
	if err := batch.ReadAtRegistered(files[0], buffers[1], 64<<10, 2, ioring.SQENone); err != nil {
		log.Fatal(err)
	}
	completions := make([]ioring.Completion, batch.Len())
	if _, err := batch.Execute(completions); err != nil {
		log.Fatal(err)
	}
	for _, completion := range completions {
		if completion.ResultCode.Failed() {
			log.Printf("request %d: %v", completion.UserData, completion.ResultCode)
		}
	}
}
