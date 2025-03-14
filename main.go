package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {

	// uses built in flag support
	clustername := flag.String("clustername", "infrastructure", "The name of sentinel cluster to generate data in and poll")
	dummyDataKey := flag.String("datakey", "dummy-set", "The name of the key to use, where data should be generated if it doesn't already exist")
	sentinelAddr := flag.String("sentineladdr", "redis-sentinel.service.consul:26379", "The path and port to sentinel services")
	tickTime := flag.String("ticktime", "100ms", "The tick interval used to hit each replica and issue a scard on the 'datakey'. Input is parsed using stdlib duration funcs.")

	// parse any inputs provided to the executable
	flag.Parse()

	// convert the tickTime to a duration
	parsedTickTime, err := time.ParseDuration(*tickTime)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("unable to parse time input %v, err: %v", *tickTime, err))
		os.Exit(-1)
	}

	// establish context with a defered cancel func call
	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// establish a channel, wire up specific signal monitoring to it, spin a routine to listen on channel and cancel the main context if caught
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-term
		fmt.Println(fmt.Sprintf("shutting down..."))
		cancelFunc()
	}()

	// cluster client used to create the entries of approx 400mb of data
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    *clustername,
		SentinelAddrs: []string{*sentinelAddr}})
	defer client.Close()

	// if the dummy data does not exist, created it
	dummyDataExists := client.Exists(ctx, *dummyDataKey)
	if dummyDataExists.Val() == 0 {
		println(fmt.Sprintf("Creating ~400MB of data in the set, key: %v", *dummyDataKey))
		for j := 1; j <= 8_500; j++ {
			cmd := client.SAdd(ctx, *dummyDataKey, stringWithCharset(50_000, charset))
			_, err := cmd.Result()
			if err != nil {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("unable to create data in the set %v, err: %v", *dummyDataKey, err))
				os.Exit(-1)
			}
		}
		println(fmt.Sprintf("Finished creating data at key: %v", *dummyDataKey))
	}

	// create sentinel client to resolve the replicas
	sentinelClient := redis.NewSentinelClient(&redis.Options{
		Addr: *sentinelAddr,
	})
	defer sentinelClient.Close()

	replicas, _ := sentinelClient.Replicas(ctx, *clustername).Result()
	replicaAddresses := parseReplicaAddrs(replicas, false)

	redisClientErrGroup, errGroupCtx := errgroup.WithContext(ctx)

	ticker := time.NewTicker(parsedTickTime)

	for _, address := range replicaAddresses {
		internalLoopAddress := address
		redisClientErrGroup.Go(func() error {
			//println(fmt.Sprintf("creating a redis client for replica at: %v", internalLoopAddress))
			replicaClient := redis.NewClient(&redis.Options{
				Addr:         internalLoopAddress,
				Password:     "",
				DB:           0,
				DialTimeout:  30 * time.Second,
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
			})
			defer replicaClient.Close()
			//println(fmt.Sprintf("redis client established for replica at: %v", internalLoopAddress))

			lastQueryFailed := false

			for {
				select {
				case <-errGroupCtx.Done():
					return nil
				case <-ticker.C:
					now := time.Now()
					scardResult, err := replicaClient.SCard(errGroupCtx, *dummyDataKey).Result()
					if err != nil {
						if !lastQueryFailed {
							println(fmt.Sprintf("%v an error occured talking to instance: %v, err: %v", now.Format(time.RFC3339Nano), internalLoopAddress, err))
						}
						lastQueryFailed = true
					} else {
						if lastQueryFailed {
							println(fmt.Sprintf("%v success result from instance: %v, scard result: %v", now.Format(time.RFC3339Nano), internalLoopAddress, scardResult))
						}
						lastQueryFailed = false
					}
				}
			}
		})
	}

	redisClientErrGroup.Wait()
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func parseReplicaAddrs(addrs []map[string]string, keepDisconnected bool) []string {
	nodes := make([]string, 0, len(addrs))
	for _, node := range addrs {
		isDown := false
		if flags, ok := node["flags"]; ok {
			for _, flag := range strings.Split(flags, ",") {
				switch flag {
				case "s_down", "o_down":
					isDown = true
				case "disconnected":
					if !keepDisconnected {
						isDown = true
					}
				}
			}
		}
		if !isDown && node["ip"] != "" && node["port"] != "" {
			nodes = append(nodes, net.JoinHostPort(node["ip"], node["port"]))
		}
	}

	return nodes
}
