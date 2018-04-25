package process

import "sync"

// Process inteface
type Process interface {
	Run() error
	Stop() error
}

// Group is used to wait for a group of processes to finish.
// It is much like a sync.WaitGroup, but adds a way to gracefully shutdown
// all the processes.
type Group struct {
	wg        sync.WaitGroup
	mu        sync.Mutex
	stopped   bool
	stopErr   error
	processes []Process
}

// NewGroup creates ProcessGroup instance
func NewGroup() *Group {
	return &Group{
		processes: make([]Process, 0),
	}
}

// AddAndRun adds and runs processes in the group.
func (psg *Group) AddAndRun(processes ...Process) {
	psg.mu.Lock()
	defer psg.mu.Unlock()
	if psg.stopped {
		return
	}

	psg.processes = append(psg.processes, processes...)
	for _, ps := range processes {
		psg.wg.Add(1)
		go func(ps Process) {
			defer psg.wg.Done()
			err := ps.Run()

			if err != ErrShutdown {
				psg.updateStopError(err)
			}

			if err != nil {
				psg.Stop()
			}
		}(ps)
	}
}

func (psg *Group) updateStopError(err error) {
	defer psg.mu.Unlock()
	psg.mu.Lock()
	if psg.stopErr != nil {
		return
	}

	psg.stopErr = err
}

// Wait until all processes are stopped
func (psg *Group) Wait() error {
	psg.wg.Wait()

	defer psg.mu.Unlock()
	psg.mu.Lock()
	return psg.stopErr
}

func (psg *Group) stopLocked() {
	if psg.stopped {
		return
	}

	psg.stopped = true

	for _, ps := range psg.processes {
		ps.Stop()
	}
}

// Stop all processes
func (psg *Group) Stop() {
	psg.mu.Lock()
	defer psg.mu.Unlock()
	psg.stopLocked()
}
