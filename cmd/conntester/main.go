package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	_ "github.com/lib/pq"
)

const (
	// Metric names
	attemptCountMetric      = "chalk.conntester.attempt_count"
	connectionLatencyMetric = "chalk.conntester.duration"
	queryLatencyMetric      = "chalk.conntester.test_query_duration"

	// Default timeout in seconds
	defaultTimeout = 5
)

func main() {
	// Parse command line arguments
	pgURI := flag.String("uri", "", "PostgreSQL connection URI (required)")
	timeout := flag.Int("timeout", defaultTimeout, "Connection timeout in seconds")
	statsdAddr := flag.String("statsd", "127.0.0.1:8125", "StatsD server address")
	repeat := flag.Float64("repeat", 0, "Repeat delay in seconds (0 = no repeat, default 1 second if used without value)")
	tags := flag.String("tags", "", "Custom tags in format k:v,k:v to add to metrics")
	flag.Parse()

	// Validate required parameters
	if *pgURI == "" {
		fmt.Println("Error: PostgreSQL URI is required")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize StatsD client
	client, err := statsd.New(*statsdAddr)
	if err != nil {
		log.Fatalf("Failed to initialize StatsD client: %v", err)
	}
	defer client.Close()

	// Set client namespace prefix
	client.Namespace = ""

	// Parse custom tags
	customTags := parseTags(*tags)

	// Test the connection once or repeatedly
	if *repeat > 0 {
		// If repeat is specified but very small, default to 1 second
		delay := *repeat
		if delay < 0.001 {
			delay = 1.0
		}
		
		fmt.Printf("Starting repeated connection tests every %.3f seconds...\n", delay)
		ticker := time.NewTicker(time.Duration(delay * float64(time.Second)))
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				runConnectionTest(*pgURI, *timeout, client, customTags)
			}
		}
	} else {
		success, _ := runConnectionTest(*pgURI, *timeout, client, customTags)

		if success {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}
}

func testConnection(pgURI string, timeoutSeconds int, client *statsd.Client, customTags []string) (bool, time.Duration, time.Duration) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	// Record start time
	startTime := time.Now()

	// Open connection
	db, err := sql.Open("postgres", pgURI)
	if err != nil {
		log.Printf("Failed to create database connection: %v", err)

		// Use a copy of customTags to avoid modifying the original
		tags := make([]string, len(customTags))
		copy(tags, customTags)
		
		// Emit metric with status:failure
		statusAdded := false
		for i, tag := range tags {
			if strings.HasPrefix(tag, "status:") {
				tags[i] = "status:failure"
				statusAdded = true
				break
			}
		}
		
		if !statusAdded {
			tags = append(tags, "status:failure")
		}
		
		if emitErr := client.Incr(attemptCountMetric, tags, 1); emitErr != nil {
			log.Printf("Failed to emit failure metric: %v", emitErr)
		}
		return false, time.Since(startTime), 0
	}
	defer db.Close()

	// Ping to verify connection is successful and calculate connection time
	err = db.PingContext(ctx)

	// Calculate elapsed time
	elapsedTime := time.Since(startTime)

	// Determine success or failure
	success := err == nil
	status := "success"
	if !success {
		status = "failure"
		log.Printf("Connection failed: %v", err)
	}

	// Use a copy of customTags to avoid modifying the original
	tags := make([]string, len(customTags))
	copy(tags, customTags)
	
	// Add or replace status tag
	statusAdded := false
	for i, tag := range tags {
		if strings.HasPrefix(tag, "status:") {
			tags[i] = fmt.Sprintf("status:%s", status)
			statusAdded = true
			break
		}
	}
	
	if !statusAdded {
		tags = append(tags, fmt.Sprintf("status:%s", status))
	}

	// Record connection latency as distribution
	if err := client.Distribution(connectionLatencyMetric, elapsedTime.Seconds(), tags, 1); err != nil {
		log.Printf("Failed to emit latency metric: %v", err)
	}

	// Record attempt count with final status
	if err := client.Incr(attemptCountMetric, tags, 1); err != nil {
		log.Printf("Failed to emit attempt metric: %v", err)
	}

	// If connection was successful, run a test query and measure its latency
	var queryLatency time.Duration
	if success {
		queryStart := time.Now()
		var testResult int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&testResult)
		queryLatency = time.Since(queryStart)
		
		if err != nil {
			log.Printf("Test query failed: %v", err)
			// Query failed, but connection was successful
			queryTags := make([]string, len(customTags))
			copy(queryTags, customTags)
			
			// Add query status failure
			queryStatusAdded := false
			for i, tag := range queryTags {
				if strings.HasPrefix(tag, "status:") {
					queryTags[i] = "status:query_failure"
					queryStatusAdded = true
					break
				}
			}
			
			if !queryStatusAdded {
				queryTags = append(queryTags, "status:query_failure")
			}
			
			// Record query latency even on failure
			if err := client.Distribution(queryLatencyMetric, queryLatency.Seconds(), queryTags, 1); err != nil {
				log.Printf("Failed to emit query latency metric: %v", err)
			}
		} else {
			// Query successful
			queryTags := make([]string, len(customTags))
			copy(queryTags, customTags)
			
			// Add query status success
			queryStatusAdded := false
			for i, tag := range queryTags {
				if strings.HasPrefix(tag, "status:") {
					queryTags[i] = "status:success"
					queryStatusAdded = true
					break
				}
			}
			
			if !queryStatusAdded {
				queryTags = append(queryTags, "status:success")
			}
			
			// Record query latency
			if err := client.Distribution(queryLatencyMetric, queryLatency.Seconds(), queryTags, 1); err != nil {
				log.Printf("Failed to emit query latency metric: %v", err)
			}
		}
	}

	return success, elapsedTime, queryLatency
}

func runConnectionTest(pgURI string, timeoutSeconds int, client *statsd.Client, customTags []string) (bool, time.Duration) {
	success, latency, queryLatency := testConnection(pgURI, timeoutSeconds, client, customTags)

	if success {
		if queryLatency > 0 {
			fmt.Printf("Connection test completed successfully (connection: %.3fms, query: %.3fms)\n", 
				float64(latency.Microseconds())/1000, float64(queryLatency.Microseconds())/1000)
		} else {
			fmt.Printf("Connection test completed successfully (connection: %.3fms)\n", float64(latency.Microseconds())/1000)
		}
	} else {
		fmt.Printf("Connection test failed (latency: %.3fms)\n", float64(latency.Microseconds())/1000)
	}

	return success, latency
}

// parseTags parses a string in the format "k1:v1,k2:v2" into a slice of "k1:v1", "k2:v2"
func parseTags(tagsStr string) []string {
	if tagsStr == "" {
		return nil
	}
	
	var result []string
	tagPairs := strings.Split(tagsStr, ",")
	
	for _, pair := range tagPairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		
		// Only add pairs that have the format k:v
		if strings.Contains(pair, ":") {
			result = append(result, pair)
		}
	}
	
	return result
}
