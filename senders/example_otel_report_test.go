package senders_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Example_otelReport demonstrates how to send Wavefront metrics to an OTel collector's /report endpoint
func Example_otelReport() {
	// OTel collector endpoint
	otelURL := "http://localhost:8085/report"
	tenantID := "16"

	// Wavefront metric format: "metric.name" value timestamp source="source" "tag"="value"
	metrics := []string{
		`"cpu.usage" 85.5 1234567890 source="server-01" "env"="prod" "region"="us-west"`,
		`"memory.used" 4096 1234567890 source="server-01" "env"="prod"`,
		`"disk.free" 50000 1234567890 source="server-02" "env"="staging"`,
	}

	// Join metrics with newlines
	payload := ""
	for i, metric := range metrics {
		if i > 0 {
			payload += "\n"
		}
		payload += metric
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", otelURL, bytes.NewBufferString(payload))
	if err != nil {
		fmt.Printf("Failed to create request: %v\n", err)
		return
	}

	// Set headers
	// Content-Type can be: text/plain, application/octet-stream, or application/x-www-form-urlencoded
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("dx_tenant_id", tenantID)

	// Send request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Failed to send request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// Read response
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
		fmt.Printf("Successfully sent %d metrics\n", len(metrics))
	} else {
		fmt.Printf("Failed with status %d: %s\n", resp.StatusCode, string(body))
	}
}

// Example_otelReportBatched demonstrates sending metrics in batches for better performance
func Example_otelReportBatched() {
	otelURL := "http://localhost:8085/report"
	tenantID := "16"
	batchSize := 1000

	// Simulate a large number of metrics
	var allMetrics []string
	for i := 0; i < 5000; i++ {
		metric := fmt.Sprintf(`"test.metric.%d" %d %d source="test-source" "batch"="true"`,
			i, i*10, time.Now().Unix())
		allMetrics = append(allMetrics, metric)
	}

	// Send in batches
	totalSent := 0
	for i := 0; i < len(allMetrics); i += batchSize {
		end := i + batchSize
		if end > len(allMetrics) {
			end = len(allMetrics)
		}

		batch := allMetrics[i:end]
		payload := ""
		for j, metric := range batch {
			if j > 0 {
				payload += "\n"
			}
			payload += metric
		}

		req, err := http.NewRequest("POST", otelURL, bytes.NewBufferString(payload))
		if err != nil {
			fmt.Printf("Failed to create request: %v\n", err)
			continue
		}

		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("dx_tenant_id", tenantID)

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Failed to send batch: %v\n", err)
			continue
		}

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
			totalSent += len(batch)
		}
		resp.Body.Close()
	}

	fmt.Printf("Successfully sent %d metrics in batches of %d\n", totalSent, batchSize)
}

// Example_otelReportReplay demonstrates replaying metrics multiple times with sleep intervals
func Example_otelReportReplay() {
	otelURL := "http://localhost:8085/report"
	tenantID := "16"

	// Configuration for replaying
	replayCount := 3                // Replay 3 times
	sleepBetween := 1 * time.Second // 1 second sleep between replays
	batchSize := 1000

	// Sample metrics
	metrics := []string{
		`"cpu.usage" 85.5 1234567890 source="server-01" "env"="prod"`,
		`"memory.used" 4096 1234567890 source="server-01" "env"="prod"`,
		`"disk.free" 50000 1234567890 source="server-02" "env"="staging"`,
	}

	totalSent := 0

	// Replay loop
	for replay := 1; replay <= replayCount; replay++ {
		fmt.Printf("Starting replay %d/%d\n", replay, replayCount)

		// Send metrics in batches
		for i := 0; i < len(metrics); i += batchSize {
			end := i + batchSize
			if end > len(metrics) {
				end = len(metrics)
			}

			batch := metrics[i:end]
			payload := ""
			for j, metric := range batch {
				if j > 0 {
					payload += "\n"
				}
				payload += metric
			}

			req, err := http.NewRequest("POST", otelURL, bytes.NewBufferString(payload))
			if err != nil {
				fmt.Printf("Failed to create request: %v\n", err)
				continue
			}

			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("dx_tenant_id", tenantID)

			client := &http.Client{Timeout: 30 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("Failed to send batch: %v\n", err)
				continue
			}

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				totalSent += len(batch)
			}
			resp.Body.Close()
		}

		fmt.Printf("Completed replay %d/%d\n", replay, replayCount)

		// Sleep between replays (except after the last one)
		if replay < replayCount {
			fmt.Printf("Sleeping for %v before next replay...\n", sleepBetween)
			time.Sleep(sleepBetween)
		}
	}

	fmt.Printf("Successfully sent %d metrics across %d replays\n", totalSent, replayCount)
}
