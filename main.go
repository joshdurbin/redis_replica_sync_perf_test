package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var (
	// Config variables
	cfgFile      string
	clustername  string
	dummyDataKey string
	sentinelAddr string
	tickTime     string
)

func main() {
	// Create the root command
	rootCmd := &cobra.Command{
		Use:   "redis-monitor",
		Short: "Monitor Redis replicas using Sentinel",
		Long: `A Redis monitoring tool that polls replicas at a configurable interval.
It can also generate dummy data for testing if it doesn't exist.`,
		Run: func(cmd *cobra.Command, args []string) {
			runMonitor()
		},
	}

	// Initialize config
	cobra.OnInitialize(initConfig)

	// Define flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.redis-monitor.yaml)")
	rootCmd.Flags().StringVar(&clustername, "clustername", "infrastructure", "The name of sentinel cluster to generate data in and poll")
	rootCmd.Flags().StringVar(&dummyDataKey, "datakey", "dummy-set", "The name of the key to use, where data should be generated if it doesn't already exist")
	rootCmd.Flags().StringVar(&sentinelAddr, "sentineladdr", "redis-sentinel.service.consul:26379", "The path and port to sentinel services")
	rootCmd.Flags().StringVar(&tickTime, "ticktime", "100ms", "The tick interval used to hit each replica and issue a scard on the 'datakey'")

	// Bind flags to viper
	viper.BindPFlag("clustername", rootCmd.Flags().Lookup("clustername"))
	viper.BindPFlag("datakey", rootCmd.Flags().Lookup("datakey"))
	viper.BindPFlag("sentineladdr", rootCmd.Flags().Lookup("sentineladdr"))
	viper.BindPFlag("ticktime", rootCmd.Flags().Lookup("ticktime"))

	// Execute the command
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// initConfig reads in config file and ENV variables if set
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".redis-monitor" (without extension)
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".redis-monitor")
	}

	// Read environment variables prefixed with REDIS_MONITOR_
	viper.SetEnvPrefix("REDIS_MONITOR")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// If a config file is found, read it in
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// runMonitor executes the main monitoring logic
func runMonitor() {
	// Get values from viper
	clustername := viper.GetString("clustername")
	dummyDataKey := viper.GetString("datakey")
	sentinelAddr := viper.GetString("sentineladdr")
	tickTime := viper.GetString("ticktime")

	// Convert the tickTime to a duration
	parsedTickTime, err := time.ParseDuration(tickTime)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to parse time input %v, err: %v\n", tickTime, err)
		os.Exit(1)
	}

	// Establish context with a deferred cancel func call
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Set up signal handling for graceful shutdown
	setupSignalHandling(cancelFunc)

	// Create Redis client for the master
	client := createFailoverClient(clustername, sentinelAddr)
	defer client.Close()

	// Create or verify dummy data
	ensureDummyDataExists(ctx, client, dummyDataKey)

	// Create sentinel client to resolve the replicas
	sentinelClient := redis.NewSentinelClient(&redis.Options{
		Addr: sentinelAddr,
	})
	defer sentinelClient.Close()

	// Get replica addresses
	replicas, err := sentinelClient.Replicas(ctx, clustername).Result()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to get replicas: %v\n", err)
		os.Exit(1)
	}
	replicaAddresses := parseReplicaAddrs(replicas, false)

	// Set up error group for monitoring replicas
	redisClientErrGroup, errGroupCtx := errgroup.WithContext(ctx)
	ticker := time.NewTicker(parsedTickTime)

	// Start monitoring each replica
	monitorReplicas(redisClientErrGroup, errGroupCtx, replicaAddresses, ticker, dummyDataKey)

	// Wait for all goroutines to complete
	if err := redisClientErrGroup.Wait(); err != nil {
		fmt.Fprintf(os.Stderr, "error in monitoring: %v\n", err)
		os.Exit(1)
	}
}

func setupSignalHandling(cancelFunc context.CancelFunc) {
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-term
		fmt.Println("shutting down...")
		cancelFunc()
	}()
}

func createFailoverClient(clustername, sentinelAddr string) *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    clustername,
		SentinelAddrs: []string{sentinelAddr},
	})
}

func ensureDummyDataExists(ctx context.Context, client *redis.Client, dummyDataKey string) {
	dummyDataExists := client.Exists(ctx, dummyDataKey)
	if dummyDataExists.Val() == 0 {
		fmt.Printf("Creating ~400MB of data in the set, key: %v\n", dummyDataKey)
		
		seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		
		for j := 1; j <= 8_500; j++ {
			// Generate random string
			data := stringWithCharset(50_000, charset, seededRand)
			
			cmd := client.SAdd(ctx, dummyDataKey, data)
			_, err := cmd.Result()
			if err != nil {
				fmt.Fprintf(os.Stderr, "unable to create data in the set %v, err: %v\n", dummyDataKey, err)
				os.Exit(1)
			}
		}
		fmt.Printf("Finished creating data at key: %v\n", dummyDataKey)
	}
}

func monitorReplicas(g *errgroup.Group, ctx context.Context, addresses []string, ticker *time.Ticker, dataKey string) {
	for _, address := range addresses {
		replicaAddr := address // Create a copy for the goroutine
		
		g.Go(func() error {
			replicaClient := redis.NewClient(&redis.Options{
				Addr:         replicaAddr,
				Password:     "",
				DB:           0,
				DialTimeout:  30 * time.Second,
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
			})
			defer replicaClient.Close()

			lastQueryFailed := false

			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					now := time.Now()
					scardResult, err := replicaClient.SCard(ctx, dataKey).Result()
					if err != nil {
						if !lastQueryFailed {
							fmt.Printf("%v an error occurred talking to instance: %v, err: %v\n", 
								now.Format(time.RFC3339Nano), replicaAddr, err)
						}
						lastQueryFailed = true
					} else {
						if lastQueryFailed {
							fmt.Printf("%v success result from instance: %v, scard result: %v\n", 
								now.Format(time.RFC3339Nano), replicaAddr, scardResult)
						}
						lastQueryFailed = false
					}
				}
			}
		})
	}
}

func stringWithCharset(length int, charset string, seededRand *rand.Rand) string {
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
