package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/longhorn/go-common-libs/profiler"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine/pkg/interceptor"
)

const (
	opShow    = "SHOW"
	opEnable  = "ENABLE"
	opDisable = "DISABLE"

	// ProfilerPortBase is the base number that we used to generate the profiler server port.
	// We reserve the port 10000 - 30000 for engine v1/v2.
	// We need to avoid to use them, so 20001 is big enough of the above interval.
	profilerPortBase = 20001
)

func ProfilerCmd() *cli.Command {
	return &cli.Command{
		Name: "profiler",
		Commands: []*cli.Command{
			ProfilerShowCmd(),
			ProfilerEnableCmd(),
			ProfilerDisableCmd(),
		},
	}
}

func getProfilerClient(c *cli.Command) (*profiler.Client, error) {
	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
	dialOpts := []grpc.DialOption{interceptor.WithIdentityValidationClientInterceptor(volumeName, engineInstanceName)}
	return profiler.NewClient(url, volumeName, dialOpts...)
}

func ProfilerShowCmd() *cli.Command {
	return &cli.Command{
		Name: "show",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := showProfiler(c); err != nil {
				logrus.WithError(err).Fatalf("Error running profiler show")
				return err
			}
			return nil
		},
	}
}

func ProfilerEnableCmd() *cli.Command {
	return &cli.Command{
		Name: "enable",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:     "port",
				Hidden:   false,
				Required: false,
				Value:    0,
				Usage:    "run profiler with specific port. The port number should bigger than 30000.",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := enableProfiler(c); err != nil {
				logrus.WithError(err).Fatalf("Error running profiler enable")
				return err
			}
			return nil
		},
	}
}

func ProfilerDisableCmd() *cli.Command {
	return &cli.Command{
		Name: "disable",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := disableProfiler(c); err != nil {
				logrus.WithError(err).Fatalf("Error running profiler disable")
				return err
			}
			return nil
		},
	}
}

func showProfiler(c *cli.Command) error {
	client, err := getProfilerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := client.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close profiler client")
		}
	}()

	profilerAddr, err := client.ProfilerOP(opShow, 0)
	if err != nil {
		fmt.Printf("Failed to show profiler: %v", err)
		return err
	}

	if profilerAddr == "" {
		fmt.Println("Profiler is not enabled")
	} else {
		fmt.Printf("Profiler enabled at Addr: *%v\n", profilerAddr)
	}
	return nil
}

func enableProfiler(c *cli.Command) error {
	portNumber := int32(c.Int("port"))
	if err := validatePortNumber(portNumber); err != nil {
		return err
	}

	// get grpc server port as a base port number and add profilerPortBase as target port number
	if portNumber == 0 {
		grpcURL := c.String("url")
		grpcPort, err := strconv.Atoi(strings.Split(grpcURL, ":")[1])
		if err != nil {
			return fmt.Errorf("failed to get grpc server port")
		}
		portNumber = int32(grpcPort) + profilerPortBase
	}

	client, err := getProfilerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := client.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close profiler client")
		}
	}()

	profilerAddr, err := client.ProfilerOP(opEnable, portNumber)
	if err != nil {
		fmt.Printf("Failed to enable profiler: %v", err)
		return err
	}
	fmt.Printf("Profiler enabled at Addr: *%v\n", profilerAddr)
	return nil
}

func disableProfiler(c *cli.Command) error {
	client, err := getProfilerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := client.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close profiler client")
		}
	}()

	_, err = client.ProfilerOP(opDisable, 0)
	if err != nil {
		fmt.Printf("Failed to disable profiler: %v", err)
		return err
	}
	fmt.Println("Profiler is disabled!")
	return nil
}

func validatePortNumber(portNumber int32) error {
	if portNumber > 0 && portNumber < 30000 {
		return fmt.Errorf("port number should be bigger than 30000. The reserved port number is [10000 - 30000]")
	}
	return nil
}
