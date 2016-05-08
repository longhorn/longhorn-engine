package agent

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/pkg/reexec"
	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

type Server struct {
	sync.Mutex

	processCounter     int
	currentPort        int
	startPort, endPort int
	nextProcess        int
	processes          map[string]*Process
	processesByPort    map[int]*Process
}

func NewServer(start, end int) *Server {
	return &Server{
		currentPort:     start,
		startPort:       start,
		endPort:         end,
		processes:       map[string]*Process{},
		processesByPort: map[int]*Process{},
	}
}

func (s *Server) ListProcesses(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	resp := ProcessCollection{
		Collection: client.Collection{
			ResourceType: "process",
		},
	}

	s.Lock()
	for _, p := range s.processes {
		resp.Data = append(resp.Data, *p)
	}
	s.Unlock()

	apiContext.Write(&resp)
	return nil
}

func (s *Server) GetProcess(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	s.Lock()
	p, ok := s.processes[id]
	s.Unlock()

	if ok {
		apiContext.Write(&p)
	} else {
		rw.WriteHeader(http.StatusNotFound)
	}

	return nil
}

func (s *Server) CreateProcess(rw http.ResponseWriter, req *http.Request) error {
	var p Process
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&p); err != nil {
		return err
	}

	s.Lock()

	if p.SrcFile == "" {
		var err error
		p.Port, err = s.nextPort()
		if err != nil {
			s.Unlock()
			return err
		}
	}

	s.processCounter++
	id := strconv.Itoa(s.processCounter)
	p.Id = id
	p.Type = "process"
	s.processes[p.Id] = &p
	s.processesByPort[p.Port] = &p

	s.Unlock()

	p.ExitCode = -2
	go func() {
		if err := s.launch(&p); err != nil {
			logrus.Errorf("Failed to launch %#v: %v", p, err)
		}
	}()

	apiContext.Write(&p)
	return nil
}

func (s *Server) launch(p *Process) error {
	switch p.ProcessType {
	case "sync":
		return s.launchSync(p)
	case "fold":
		return s.launchFold(p)
	}
	return fmt.Errorf("Unknown process type %s", p.ProcessType)
}

func (s *Server) launchFold(p *Process) error {
	cmd := reexec.Command("sfold", p.SrcFile, p.DestFile)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	logrus.Infof("Running %s %v", cmd.Path, cmd.Args)
	err := cmd.Wait()
	if err != nil {
		logrus.Infof("Error running %s %v: %v", "sfold", cmd.Args, err)
		p.ExitCode = 1
		if exitError, ok := err.(*exec.ExitError); ok {
			if waitStatus, ok := exitError.Sys().(syscall.WaitStatus); ok {
				logrus.Infof("Error running %s %v: %v", "sfold", cmd.Args, waitStatus.ExitStatus())
				p.ExitCode = waitStatus.ExitStatus()
			}
		}
		return err
	}

	p.ExitCode = 0
	logrus.Infof("Done running %s %v", "sfold", cmd.Args)
	return nil
}

func binName() (string, error) {
	if _, err := os.Stat(os.Args[0]); err == nil {
		return os.Args[0], nil
	}
	return exec.LookPath(os.Args[0])
}

func (s *Server) launchSync(p *Process) error {
	args := []string{"ssync"}
	if p.Host != "" {
		args = append(args, "-host", p.Host)
	}
	if p.Port != 0 {
		args = append(args, "-port", strconv.Itoa(p.Port))
	}
	if p.SrcFile == "" {
		args = append(args, "-daemon")
	} else {
		args = append(args, p.SrcFile)
		if p.DestFile != "" {
			args = append(args, p.DestFile)
		}
	}

	cmd := reexec.Command(args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	logrus.Infof("Running %s %v", "ssync", args)
	err := cmd.Wait()
	if err != nil {
		logrus.Infof("Error running %s %v: %v", "ssync", args, err)
		p.ExitCode = 1
		if exitError, ok := err.(*exec.ExitError); ok {
			if waitStatus, ok := exitError.Sys().(syscall.WaitStatus); ok {
				logrus.Infof("Error running %s %v: %v", "ssync", args, waitStatus.ExitStatus())
				p.ExitCode = waitStatus.ExitStatus()
			}
		}
		return err
	}

	p.ExitCode = 0
	logrus.Infof("Done running %s %v", "ssync", args)
	return nil
}

func (s *Server) nextPort() (int, error) {
	// Must be called with s.Lock() obtained
	for i := 0; i < (s.endPort - s.startPort + 1); i++ {
		port := s.currentPort
		s.currentPort++
		if s.currentPort > s.endPort {
			s.currentPort = s.startPort
		}

		if _, ok := s.processesByPort[port]; ok {
			continue
		}

		return port, nil
	}

	return 0, errors.New("Out of ports")
}
