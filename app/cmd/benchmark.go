package cmd

import (
	"fmt"
	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func BenchmarkCmd() cli.Command {
	return cli.Command{
		Name: "bench",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "instance-type",
				Usage: "Longhorn instance type, engine or replica",
			},
			cli.StringFlag{
				Name:  "bench-type,b",
				Usage: "The type can be <seq>/<rand>-<iops>/<bandwidth>/<latency>-<read>/<write>. For example, seq-bandwidth-write.",
			},
			cli.IntFlag{
				Name:  "thread,t",
				Value: 1,
				Usage: "The concurrent thread count. For latency related benchmarks, this value will be forcibly set to 1.",
			},
			cli.Int64Flag{
				Name:  "size",
				Value: 1 << 30,
				Usage: "The test size. " +
					"If there are multi-thread enabled, this cmd will evenly split the test size for each thread. And each thread should have at least one block handled." +
					"For bandwidth benchmarks, the block size is 1MB. Other benchmarks use 4K-block instead.",
			},
		},
		Usage: "Benchmark engine or replica IO performance. When benchmarking engine with this cmd, the frontend iSCSI will be bypassed. When benchmarking replica, both the frontend iSCSI and the engine-replica connection will be bypassed.",
		Action: func(c *cli.Context) {
			if err := bench(c); err != nil {
				logrus.WithError(err).Fatalf("Error running iops command")
			}
		},
	}
}

func bench(c *cli.Context) error {
	instanceType := c.String("instance-type")
	benchType := c.String("bench-type")
	threadCnt := c.Int("thread")
	size := c.Int64("size")

	var output string

	switch instanceType {
	case "engine":
		controllerClient, err := getControllerClient(c)
		if err != nil {
			return err
		}
		defer controllerClient.Close()
		output, err = controllerClient.VolumeBench(benchType, threadCnt, size)
		if err != nil {
			return err
		}
	case "replica":
		url := c.GlobalString("url")
		volumeName := c.GlobalString("volume-name")
		repClient, err := replicaClient.NewReplicaClient(url, volumeName, "")
		if err != nil {
			return err
		}
		defer repClient.Close()
		output, err = repClient.Bench(benchType, threadCnt, size)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid bench type %s", benchType)
	}

	fmt.Println(output)
	return nil
}
