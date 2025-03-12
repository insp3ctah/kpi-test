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
	httpTimeout = 30 * time.Second // better use an 1.5std of avg response time
)

type Fact struct {
	PeriodStart         string
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

func sendFact(f Fact) error {
	data := url.Values{}
	data.Set("period_start", f.PeriodStart)
	data.Set("period_end", f.PeriodEnd)
	data.Set("period_key", f.PeriodKey)
	data.Set("indicator_to_mo_id", fmt.Sprintf("%d", f.IndicatorToMoID))
	data.Set("indicator_to_mo_fact_id", fmt.Sprintf("%d", f.IndicatorToMoFactID))
	data.Set("value", fmt.Sprintf("%d", f.Value))
	data.Set("fact_time", f.FactTime)
	data.Set("is_plan", fmt.Sprintf("%d", f.IsPlan))
	data.Set("auth_user_id", fmt.Sprintf("%d", f.AuthUserID))
	data.Set("comment", f.Comment)

	req, err := http.NewRequest("POST", apiURL, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error: status code: %d", resp.StatusCode)
	}
	log.Println("Fact sent successfully", f)
	return nil
}

type Buffer struct {
	factQueue chan Fact
	wg        sync.WaitGroup
}

func NewBuffer(cap int) *Buffer {
	return &Buffer{
		factQueue: make(chan Fact, cap),
	}
}

func (b *Buffer) Add(facts []Fact) {
	b.wg.Add(len(facts))
	for _, f := range facts {
		b.factQueue <- f
	}
}

func (b *Buffer) Process() {
	for f := range b.factQueue {
		err := sendFact(f)
		if err != nil {
			log.Printf("Error sending fact: %v", err)
		}
		b.wg.Done()
	}
}

func (b *Buffer) Wait() {
	b.wg.Wait()
}

func (b *Buffer) Close() {
	close(b.factQueue)
}

func main() {
	buffer := NewBuffer(bufferSize)
	go buffer.Process()

	facts := make([]Fact, 10)
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
			Comment:             "jingle bells",
		}
	}

	buffer.Add(facts)
	buffer.Wait()
	buffer.Close()

	fmt.Println("All facts sent successfully")
}
