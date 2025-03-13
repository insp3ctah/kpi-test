package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

const (
	getFactsURL = "https://development.kpi-drive.ru/_api/indicators/get_facts"
	token       = "48ab34464a5573519725deb5865cc74c"
)

func main() {
	// Формируем данные запроса
	data := url.Values{
		"period_start":       {"2024-12-01"},
		"period_end":         {"2024-12-31"},
		"period_key":         {"month"},
		"indicator_to_mo_id": {"227373"},
	}

	req, _ := http.NewRequest("POST", getFactsURL, bytes.NewBufferString(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Request failed:", err)
	}
	defer resp.Body.Close()

	// Читаем сырой ответ
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response body:", err)
	}

	fmt.Println("HTTP Status:", resp.Status)
	fmt.Println("Headers:", resp.Header)
	fmt.Println("\nRaw response body:")
	fmt.Println(string(body))
}
