package senders_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	otelServerURL     = "http://localhost:8085/report"
	tenantID          = "16"
	metricsDataFolder = "./test/wf-dumps/" // Base folder for test metrics data
)

// ReplayConfig holds configuration for replaying metrics
type ReplayConfig struct {
	ReplayCount  int           // Number of times to replay each file (default: 1)
	SleepBetween time.Duration // Sleep duration between file replays (default: 1 second)
	BatchSize    int           // Number of lines per batch (default: 5000)
	ContentType  string        // HTTP Content-Type header (default: application/octet-stream)
}

// DefaultReplayConfig returns the default replay configuration
func DefaultReplayConfig() ReplayConfig {
	return ReplayConfig{
		ReplayCount:  1,
		SleepBetween: 1 * time.Second,
		BatchSize:    5000,
		ContentType:  "application/octet-stream",
	}
}

// TestOTelReport_SingleMetric tests sending a single metric to the OTel server
func TestOTelReport_SingleMetric(t *testing.T) {
	metric := `"test.metric" 100 1234567890 source="test-host"`

	contentTypes := []string{
		"text/plain",
		"application/octet-stream",
		"application/x-www-form-urlencoded",
	}

	for _, contentType := range contentTypes {
		t.Run(contentType, func(t *testing.T) {
			err := sendToOTel(metric, contentType)
			if err != nil {
				t.Errorf("Failed to send metric with content-type %s: %v", contentType, err)
			} else {
				t.Logf("Successfully sent metric with content-type: %s", contentType)
			}
		})
	}
}

// TestOTelReport_FormUrlEncoded_Encoding verifies that raw format (not URL-encoded) is required
func TestOTelReport_FormUrlEncoded_Encoding(t *testing.T) {
	testCases := []struct {
		name        string
		payload     string
		shouldPass  bool
		description string
	}{
		{
			name:        "raw_format_single_metric",
			payload:     `"test.metric" 100 1234567890 source="test-host"`,
			shouldPass:  true,
			description: "Raw Wavefront format works with form-urlencoded content type",
		},
		{
			name:        "raw_format_with_tags",
			payload:     `"cpu.usage" 85.5 1234567890 source="server-01" "env"="prod" "region"="us-west"`,
			shouldPass:  true,
			description: "Raw format with tags works",
		},
		{
			name: "raw_multiline",
			payload: `"cpu.usage" 85.5 1234567890 source="server-01" "env"="prod"
"memory.used" 4096 1234567890 source="server-01" "env"="prod"
"disk.free" 50000 1234567890 source="server-02" "env"="staging"`,
			shouldPass:  true,
			description: "Multiple metrics in raw format work",
		},
		{
			name:        "url_encoded_format",
			payload:     `%22test.metric%22+100+1234567890+source%3D%22test-host%22`,
			shouldPass:  false,
			description: "URL-encoded format is NOT supported (returns 400)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := sendToOTel(tc.payload, "application/x-www-form-urlencoded")
			if tc.shouldPass {
				if err != nil {
					t.Errorf("Expected success but got error: %v. %s", err, tc.description)
				} else {
					t.Logf("✓ %s", tc.description)
				}
			} else {
				if err != nil {
					t.Logf("✓ Expected failure confirmed: %s", tc.description)
				} else {
					t.Errorf("Expected failure but got success. %s", tc.description)
				}
			}
		})
	}

	// Log important finding
	t.Log("IMPORTANT: Use raw Wavefront line protocol format, NOT URL-encoded, even with application/x-www-form-urlencoded Content-Type")
}

// TestOTelReport_5000Lines tests sending 5000 lines from the wavefront metrics dump
func TestOTelReport_5000Lines(t *testing.T) {
	metricsFile := metricsDataFolder + "locust-loadgen-0/dxotel-metrics-0.txt.log"

	// Read first 5000 lines
	lines, err := readLines(metricsFile, 5000)
	if err != nil {
		t.Fatalf("Failed to read metrics file: %v", err)
	}

	t.Logf("Read %d lines from %s", len(lines), metricsFile)

	// Join lines into a single payload
	payload := strings.Join(lines, "\n")

	// Send with different content types
	contentTypes := []string{
		"text/plain",
		"application/octet-stream",
	}

	for _, contentType := range contentTypes {
		t.Run(contentType, func(t *testing.T) {
			start := time.Now()
			err := sendToOTel(payload, contentType)
			duration := time.Since(start)

			if err != nil {
				t.Errorf("Failed to send 5000 lines with content-type %s: %v", contentType, err)
			} else {
				t.Logf("Successfully sent 5000 lines with content-type %s in %v", contentType, duration)
			}
		})
	}
}

func TestOTelReport__OctetStream_AllFiles(t *testing.T) {
	metricsDir := metricsDataFolder + "locust-loadgen-0"
	config := DefaultReplayConfig()
	sendOTelReport_UsingAllFiles(t, metricsDir, config)
}

func TestOTelReport__FormUrlEncoded_AllFiles(t *testing.T) {
	metricsDir := metricsDataFolder + "locust-loadgen-0"
	config := DefaultReplayConfig()
	config.ContentType = "application/x-www-form-urlencoded"
	sendOTelReport_UsingAllFiles(t, metricsDir, config)
}

// sendOTelReport_UsingAllFiles sends all metrics from all files in batches
func sendOTelReport_UsingAllFiles(t *testing.T, metricsDir string, config ReplayConfig) {
	files, err := os.ReadDir(metricsDir)
	if err != nil {
		t.Fatalf("Failed to read metrics directory: %v", err)
	}

	totalLines := 0
	totalFiles := 0
	totalBatches := 0
	totalReplays := 0

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".txt.log") {
			continue
		}

		filePath := fmt.Sprintf("%s/%s", metricsDir, file.Name())

		t.Run(file.Name(), func(t *testing.T) {
			lines, err := readAllLines(filePath)
			if err != nil {
				t.Errorf("Failed to read file %s: %v", file.Name(), err)
				return
			}

			fileLines := 0
			fileBatches := 0

			// Replay the file multiple times
			for replay := 1; replay <= config.ReplayCount; replay++ {
				if config.ReplayCount > 1 {
					t.Logf("Replay %d/%d for %s", replay, config.ReplayCount, file.Name())
				}

				// Send in batches
				for i := 0; i < len(lines); i += config.BatchSize {
					end := i + config.BatchSize
					if end > len(lines) {
						end = len(lines)
					}

					batch := lines[i:end]
					payload := strings.Join(batch, "\n")

					start := time.Now()
					err = sendToOTel(payload, config.ContentType)
					duration := time.Since(start)

					if err != nil {
						t.Errorf("Failed to send batch from %s (replay %d): %v", file.Name(), replay, err)
						return
					}

					fileBatches++
					fileLines += len(batch)
					t.Logf("Sent batch %d: %d lines in %v", fileBatches, len(batch), duration)
				}

				// Sleep between replays (except after the last replay)
				if replay < config.ReplayCount {
					t.Logf("Sleeping for %v before next replay...", config.SleepBetween)
					time.Sleep(config.SleepBetween)
				}
			}

			t.Logf("Successfully sent %d lines from %s in %d batches (%d replays)",
				fileLines, file.Name(), fileBatches, config.ReplayCount)
			totalLines += fileLines
			totalFiles++
			totalBatches += fileBatches
			totalReplays += config.ReplayCount
		})
	}

	t.Logf("Total: Sent %d lines from %d files in %d batches (%d total replays)",
		totalLines, totalFiles, totalBatches, totalReplays)
}

// TestOTelReport_Batched tests sending metrics in batches
func TestOTelReport_Batched(t *testing.T) {
	metricsFile := metricsDataFolder + "locust-loadgen-0/dxotel-metrics-0.txt.log"
	batchSize := 1000
	contentType := "application/octet-stream"

	lines, err := readAllLines(metricsFile)
	if err != nil {
		t.Fatalf("Failed to read metrics file: %v", err)
	}

	t.Logf("Read %d lines, sending in batches of %d", len(lines), batchSize)

	totalBatches := 0
	totalSent := 0

	for i := 0; i < len(lines); i += batchSize {
		end := i + batchSize
		if end > len(lines) {
			end = len(lines)
		}

		batch := lines[i:end]
		payload := strings.Join(batch, "\n")

		start := time.Now()
		err := sendToOTel(payload, contentType)
		duration := time.Since(start)

		if err != nil {
			t.Errorf("Failed to send batch %d: %v", totalBatches+1, err)
		} else {
			t.Logf("Batch %d: Sent %d lines in %v", totalBatches+1, len(batch), duration)
			totalSent += len(batch)
			totalBatches++
		}
	}

	t.Logf("Total: Sent %d lines in %d batches", totalSent, totalBatches)
}

// TestOTelReport_ReplayWithConfig tests replaying a file multiple times with custom config
func TestOTelReport_ReplayWithConfig(t *testing.T) {
	metricsFile := metricsDataFolder + "locust-loadgen-0/dxotel-metrics-2mb.txt.log"

	// Custom configuration: replay 3 times with 2 second sleep
	config := ReplayConfig{
		ReplayCount:  3,
		SleepBetween: 2 * time.Second,
		BatchSize:    1000,
		ContentType:  "application/octet-stream",
	}

	lines, err := readAllLines(metricsFile)
	if err != nil {
		t.Fatalf("Failed to read metrics file: %v", err)
	}

	t.Logf("Read %d lines, replaying %d times with %v sleep between replays",
		len(lines), config.ReplayCount, config.SleepBetween)

	totalBatches := 0
	totalSent := 0

	// Replay the file multiple times
	for replay := 1; replay <= config.ReplayCount; replay++ {
		t.Logf("=== Starting replay %d/%d ===", replay, config.ReplayCount)
		replayStart := time.Now()

		// Send in batches
		for i := 0; i < len(lines); i += config.BatchSize {
			end := i + config.BatchSize
			if end > len(lines) {
				end = len(lines)
			}

			batch := lines[i:end]
			payload := strings.Join(batch, "\n")

			start := time.Now()
			err := sendToOTel(payload, config.ContentType)
			duration := time.Since(start)

			if err != nil {
				t.Errorf("Failed to send batch (replay %d): %v", replay, err)
				return
			}

			totalBatches++
			totalSent += len(batch)
			t.Logf("Replay %d, Batch %d: Sent %d lines in %v",
				replay, (i/config.BatchSize)+1, len(batch), duration)
		}

		replayDuration := time.Since(replayStart)
		t.Logf("=== Completed replay %d/%d in %v ===", replay, config.ReplayCount, replayDuration)

		// Sleep between replays (except after the last replay)
		if replay < config.ReplayCount {
			t.Logf("Sleeping for %v before next replay...", config.SleepBetween)
			time.Sleep(config.SleepBetween)
		}
	}

	t.Logf("Total: Sent %d lines in %d batches across %d replays",
		totalSent, totalBatches, config.ReplayCount)
}

// Helper function to send data to OTel server
func sendToOTel(payload string, contentType string) error {
	req, err := http.NewRequest("POST", otelServerURL, bytes.NewBufferString(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("dx_tenant_id", tenantID)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Helper function to read first N lines from a file
func readLines(filePath string, maxLines int) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)

	// Increase buffer size for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	count := 0
	for scanner.Scan() && count < maxLines {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
			count++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

// Helper function to read all lines from a file
func readAllLines(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)

	// Increase buffer size for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}
