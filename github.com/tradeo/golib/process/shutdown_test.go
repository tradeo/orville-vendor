package process

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tradeo/golib/log"
	"github.com/tradeo/golib/log/logtest"
)

const (
	testShutdownEnv  = "TEST_SHUTDOWN"
	shutdownHelloMsg = "hello"
	shutdownTimeout  = time.Second * 10
)

func TestShutdownStop(t *testing.T) {
	log := logtest.NewLoggerMock()
	shutdown := NewShutdown(log, time.Second*30)
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(start)
		err := shutdown.Run()
		assert.NoError(t, err)
	}()

	<-start

	shutdown.Stop()

	wg.Wait()

	assert.Equal(t, []string{"Shutdown: stopped"}, log.Infos)
}

func TestShutdownSigterm(t *testing.T) {
	log := logtest.NewLoggerMock()
	shutdown := NewShutdown(log, time.Second*30)
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(start)
		err := shutdown.Run()
		assert.EqualError(t, err, "Shutdown signal")
	}()

	<-start

	p, err := os.FindProcess(syscall.Getpid())
	if err != nil {
		t.Fatalf("Could not find own process %v", err)
		return
	}
	p.Signal(syscall.SIGTERM)

	wg.Wait()

	assert.Equal(t, []string{"Shutdown: process received signal terminated"}, log.Infos)
}

func TestShutdownStopTimeout(t *testing.T) {
	testShutdown(t, testShutdownTimeout, false)
}

func TestShutdownSigtermTimeout(t *testing.T) {
	testShutdown(t, testShutdownTimeout, true)
}

func testShutdown(t *testing.T, testFunc func(bool), sigterm bool) {
	if os.Getenv(testShutdownEnv) == "1" {
		testFunc(sigterm)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
	cmd.Env = append(os.Environ(), testShutdownEnv+"=1")

	rc, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	out := bufio.NewScanner(rc)

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// read hello message from the child process
	readErr := make(chan error, 1)
	go func() {
		out.Scan()
		if err := out.Err(); err != nil {
			readErr <- err
			return
		}

		if msg := out.Text(); msg != shutdownHelloMsg {
			readErr <- fmt.Errorf("child said %q, expected %q", msg, shutdownHelloMsg)
		}

		readErr <- nil
	}()

	select {
	case err := <-readErr:
		if err != nil {
			cmd.Process.Kill()
			t.Fatal(err)
		}
	case <-time.After(shutdownTimeout):
		cmd.Process.Kill()
		t.Fatalf("process ran for %v and did not exit", shutdownTimeout)
	}

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- cmd.Wait()
	}()

	if sigterm {
		go func() {
			cmd.Process.Signal(syscall.SIGTERM)
		}()
	}

	select {
	case err := <-waitErr:
		if err == nil {
			t.Fatalf("process ran with err %v, want exit status 1", err)
		}
	case <-time.After(shutdownTimeout):
		cmd.Process.Kill()
		t.Fatalf("process ran for %v and did not exit", shutdownTimeout)
	}
}

// func testShutdownStopEnsureTermination() {
// 	log := &log.StdLogger{}
// 	shutdown := NewShutdown(log, 0)
// 	var wg sync.WaitGroup
// 	start := make(chan struct{})

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		close(start)
// 		fmt.Println(shutdownHelloMsg)
// 		shutdown.Run()
// 	}()

// 	<-start

// 	shutdown.Stop()

// 	wg.Add(1) // the test will timeout unless the `ensure termination` works
// 	wg.Wait()
// }

func testShutdownTimeout(sigterm bool) {
	log := &log.NullLogger{}
	shutdown := NewShutdown(log, 0)
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println(shutdownHelloMsg)
		close(start)
		shutdown.Run()
	}()

	<-start

	if !sigterm {
		shutdown.Stop()
	}

	wg.Add(1) // the test will timeout unless the `ensure termination` works
	wg.Wait()
}
