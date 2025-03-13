package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	apiURL      = "https://development.kpi-drive.ru/_api/facts/save_fact"
	token       = "48ab34464a5573519725deb5865cc74c"
	bufferSize  = 1000
	httpTimeout = 30 * time.Second // Just an example; better use an 1.5std of avg response time
)

// Fact represents API request payload structure
type Fact struct {
	PeriodStart         string // Using strings instead of time.Time for simpler serialization
	PeriodEnd           string
	PeriodKey           string
	IndicatorToMoID     int
	IndicatorToMoFactID int
	Value               int
	FactTime            string
	IsPlan              int
	AuthUserID          int
	Comment             string
}

// sendFact sends a single Fact record to the API using a POST request.
func sendFact(f Fact) error {
	data := url.Values{} // url.Values efficiently encodes form data
	data.Set("period_start", f.PeriodStart)
	data.Set("period_end", f.PeriodEnd)
	data.Set("period_key", f.PeriodKey)
	data.Set("indicator_to_mo_id", fmt.Sprintf("%d", f.IndicatorToMoID)) // Numeric conversion optimized with fmt instead of strconv
	data.Set("indicator_to_mo_fact_id", fmt.Sprintf("%d", f.IndicatorToMoFactID))
	data.Set("value", fmt.Sprintf("%d", f.Value))
	data.Set("fact_time", f.FactTime)
	data.Set("is_plan", fmt.Sprintf("%d", f.IsPlan))
	data.Set("auth_user_id", fmt.Sprintf("%d", f.AuthUserID))
	data.Set("comment", f.Comment)

	// Creating new request with encoded data as the body
	req, err := http.NewRequest("POST", apiURL, bytes.NewBufferString(data.Encode())) // Reusable buffer reduces memory allocations
	if err != nil {
		return err
	}

	// Set required headers: Authorization and Content-Type once per request
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Timeout prevents resource leaks
	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req) // Send the request
	if err != nil {
		return err
	}
	defer resp.Body.Close() // Body is closed when done; critical for connection reuse

	if resp.StatusCode != http.StatusOK { // Checking if the request was correctly handled
		return fmt.Errorf("error: status code: %d", resp.StatusCode)
	}
	fmt.Println("Fact sent successfully", f) // Log success message with t
	// he sent fact
	return nil
}

// Buffer manages concurrent-safe processing pipeline
type Buffer struct {
	factQueue chan Fact      // Buffered channel (non-blocking up to bufferSize)
	wg        sync.WaitGroup // Tracks in-flight requests
}

// NewBuffer initializes and returns a new Buffer
func NewBuffer(cap int) *Buffer {
	return &Buffer{
		factQueue: make(chan Fact, cap), // Pre-allocated buffer
	}
}

// Add implements batch insertion pattern
func (b *Buffer) Add(facts []Fact) {
	b.wg.Add(len(facts)) // Atomic counter update; Sets the amount of units in wait group equal the amount of facts to be sent
	go func() {
		for _, f := range facts {
			b.factQueue <- f // Buffered write
		}
	}()
}

// Process launches consumer goroutine
func (b *Buffer) Process() {
	go func() {
		for f := range b.factQueue { // Iterating through the whole channel
			err := sendFact(f) // Sending the fact to the server with HTTP request
			if err != nil {
				log.Printf("Error sending fact: %v", err)
			}
			b.wg.Done() // Completion notification
		}
	}()
}

// Wait synchronizes completion
func (b *Buffer) Wait() {
	b.wg.Wait() // Blocks until all facts in channel are handled
}

// Close safely terminates processing
func (b *Buffer) Close() {
	close(b.factQueue) // Graceful channel shutdown; remaining facts can be read though
}

func main() {
	buffer := NewBuffer(bufferSize) // Create a new buffer with specified capacity
	buffer.Process()                // Start processing facts from the buffer in a goroutine

	facts := make([]Fact, 10) // Pre-allocate a slice of 10 test facts for testing
	for i := 0; i < 10; i++ {
		facts[i] = Fact{
			PeriodStart:         "2024-12-01",
			PeriodEnd:           "2024-12-31",
			PeriodKey:           "month",
			IndicatorToMoID:     227373,
			IndicatorToMoFactID: 0,
			Value:               i + 1,
			FactTime:            "2024-12-31",
			IsPlan:              0,
			AuthUserID:          40,
			Comment:             "buffer Last_name",
		}
	}

	buffer.Add(facts) // Async insertion
	buffer.Wait()     // Await processing
	buffer.Close()    // Clean shutdown

	fmt.Println("All facts sent successfully")
}
