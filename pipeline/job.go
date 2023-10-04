// Package pipeline implements a container type and helper functions for running parallel tasks.
//
// A pipeline.Job consists of a source of elements, zero or more functions which modify or transform those elements,
// and a consumer function which processes the final resulting elements. It is similar in operation to map/reduce.
// Pipeline jobs differ in that they can be instructed to perform any of the intermediate steps in parallel, using
// an arbitrary number of goroutines, and can be told to allow buffering of results between steps.
package pipeline

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
)

type Job[T any] struct {
	ctx     context.Context
	g       *errgroup.Group
	channel <-chan T
	running bool
}

var ErrContextCanceled = errors.New("job: context was cancelled")

// New creates a new job using the given channel as a source of items. The resulting Job will end when the
// input channel is closed by its owner.
func New[T any](ctx context.Context, in <-chan T) Job[T] {
	return Job[T]{
		g:       new(errgroup.Group),
		channel: in,
		ctx:     ctx,
	}
}

// Consumer is a function that accepts items and performs the terminal step of a pipeline. Note that Consumers should
// not hold state or expect to be notified of the completion of work, they are simple functions and when the job
// completes, the Consumer is discarded. A consumer can indicate the job has failed by returning a non-nil error.
type Consumer[T any] func(T) error

// Transformer is a function that accepts items of type IN, performs some work, and returns items of type OUT.
// A Transformer will be called repeatedly until the Job ends or the Transformer returns a non-nil error.
type Transformer[IN, OUT any] func(IN) (OUT, error)

const (
	optWorkerCount int = iota
	optBufferSize
)

type pipeOpt struct {
	workerCount int
	bufferSize  int
}

type PipeOptFn func(*pipeOpt)

// WithWorkerCount produces a PipeOptFn that sets the number of goroutines used for the given Pipe step.
func WithWorkerCount(count int) PipeOptFn {
	return func(o *pipeOpt) {
		o.workerCount = count
	}
}

// WithBufferSize produces a PipeOptFn that sets the size of the output channel used internally by the Job to pass data
// to its next step.
func WithBufferSize(size int) PipeOptFn {
	return func(o *pipeOpt) {
		o.bufferSize = size
	}
}

// Pipe adds a transformer to the job. To perform a spread of the step across multiple goroutines, include
// WithWorkerCount, which will run the transformer function on multiple goroutines. To specify a larger or smaller
// output buffer size, pass WithBufferSize. Increasing the output buffer can be useful when the next step of the pipe
// is higher latency than the current or has moments of latency interspersed with high burst processing.
func Pipe[T, U any](j Job[T], next Transformer[T, U], optFns ...PipeOptFn) Job[U] {
	if j.running {
		panic("attempted to add a Pipe to a running job")
	}
	opts := &pipeOpt{
		workerCount: 1,
		bufferSize:  -1,
	}

	for _, fn := range optFns {
		fn(opts)
	}

	if opts.bufferSize == -1 {
		opts.bufferSize = opts.workerCount
	}

	outChan := make(chan U, opts.bufferSize)

	pipeFunc := func() error {
		defer close(outChan)
		for {
			select {
			case <-j.ctx.Done():
				return ErrContextCanceled
			case item, ok := <-j.channel:
				if !ok {
					return nil
				}
				result, err := next(item)
				if err != nil {
					return err
				}
				outChan <- result
			}
		}
	}

	for i := 0; i < opts.workerCount; i++ {
		j.g.Go(pipeFunc)
	}

	return Job[U]{
		g:       j.g,
		channel: outChan,
		ctx:     j.ctx,
	}
}

// Complete adds a consumer to the job and marks it as running. Further attempts to Pipe or Complete the job will
// result in a panic
func Complete[T any](j Job[T], end Consumer[T]) {
	if j.running {
		panic("attempted to Complete a running job")
	}
	j.running = true
	endFunc := func() error {
		for {
			select {
			case <-j.ctx.Done():
				return ErrContextCanceled
			case item, ok := <-j.channel:
				if !ok {
					return nil
				}
				if err := end(item); err != nil {
					return err
				}
			}
		}
	}
	j.g.Go(endFunc)
}

func (j *Job[T]) Wait() error {
	return j.g.Wait()
}

func (j *Job[T]) IsRunning() bool {
	return j.running
}
