package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type RPCNode struct {
	Address string
	Version string
}

type EvaluatedNode struct {
	RPC     string
	Speed   float64
	Latency float64
	Slot    int
	Diff    int
	Version string
	Status  string // "good", "slow", or "bad"
}

type Config struct {
	WorkerCount      int
	MinDownloadSpeed float64 // MB/s
	MaxLatency       float64 // ms
	SleepBeforeRetry int
	DefaultSlot      int
}

var (
	cachedNodes     []EvaluatedNode
	cacheMutex      sync.Mutex
	isRefreshing    bool
	refreshMutex    sync.Mutex
	refreshInterval = 5 * time.Minute
	mainnetRPC      = "http://api.testnet.solana.com"
	config          = Config{
		WorkerCount:      100,
		MinDownloadSpeed: 50.0,  // Minimum 5 MB/s
		MaxLatency:       200.0, // Maximum 200 ms
		SleepBeforeRetry: 5,
		DefaultSlot:      0,
	}
)

const cacheFilePath = "cached_nodes.json"

func main() {
	// Load cache from disk
	err := loadCacheFromFile()
	if err != nil {
		log.Printf("Error loading cache on startup: %v", err)
	}

	// Start the cache refresh loop
	go refreshCache()

	// Setup Gin server
	r := gin.Default()

	// Routes
	r.GET("/snapshot.tar.bz2", func(c *gin.Context) {
		redirectSnapshot(c, "snapshot.tar.bz2")
	})
	r.GET("/incremental-snapshot.tar.bz2", func(c *gin.Context) {
		redirectSnapshot(c, "incremental-snapshot.tar.bz2")
	})
	r.GET("/nodes", listNodes)
	r.GET("/status", getStatus)

	log.Println("Starting Solana proxy on :5000")
	if err := r.Run(":5000"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
func refreshCache() {
	for {
		setIsRefreshing(true)
		log.Println("Starting cache refresh...")

		// Fetch cluster nodes
		nodes, err := fetchRPCNodes(mainnetRPC)
		if err != nil {
			log.Printf("Error fetching nodes: %v", err)
			setIsRefreshing(false)
			time.Sleep(refreshInterval)
			continue
		}

		// Get the latest slot for evaluation
		defaultSlot, err := getReferenceSlot(mainnetRPC)
		if err != nil {
			log.Printf("Error fetching reference slot: %v", err)
			setIsRefreshing(false)
			time.Sleep(refreshInterval)
			continue
		}
		config.DefaultSlot = defaultSlot

		// Evaluate nodes
		evaluatedNodes := evaluateNodesWithVersions(nodes, config, defaultSlot)

		// Filter only "good" nodes
		goodNodes := filterGoodNodes(evaluatedNodes)

		// Update the cache
		cacheMutex.Lock()
		cachedNodes = goodNodes
		cacheMutex.Unlock()

		// Save cache to disk
		err = saveCacheToFile()
		if err != nil {
			log.Printf("Failed to save cache to file: %v", err)
		}

		log.Printf("Cache refresh complete. Found %d good nodes.", len(goodNodes))
		setIsRefreshing(false)
		time.Sleep(refreshInterval)
	}
}

func saveCacheToFile() error {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	data, err := json.MarshalIndent(cachedNodes, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize cache: %v", err)
	}

	err = ioutil.WriteFile(cacheFilePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write cache to file: %v", err)
	}

	log.Println("Cache saved to disk.")
	return nil
}

func loadCacheFromFile() error {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	data, err := ioutil.ReadFile(cacheFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to read cache from file: %v", err)
		}
		log.Println("No cache file found. Starting fresh.")
		return nil
	}

	err = json.Unmarshal(data, &cachedNodes)
	if err != nil {
		return fmt.Errorf("failed to deserialize cache: %v", err)
	}

	log.Printf("Loaded %d nodes from cache file.", len(cachedNodes))
	return nil
}

func redirectSnapshot(c *gin.Context, snapshotType string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	if len(cachedNodes) == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "No available nodes. Cache is empty."})
		return
	}

	// Select the best RPC node
	bestRPC := selectBestRPC(cachedNodes)
	if bestRPC == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "No good nodes found."})
		return
	}

	// Construct the URL for the snapshot
	snapshotURL := fmt.Sprintf("%s/%s", bestRPC, snapshotType)

	// Perform a HEAD request to get the final filename from the upstream node
	resp, err := http.Head(snapshotURL)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Printf("Error getting snapshot HEAD from %s: %v", snapshotURL, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("Failed to fetch snapshot HEAD from %s", bestRPC)})
		return
	}
	defer resp.Body.Close()

	// Extract the file name from the upstream response
	fileName := filepath.Base(snapshotURL) // Default to the base of the URL
	if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
		_, params, err := mime.ParseMediaType(contentDisposition)
		if err == nil && params["filename"] != "" {
			fileName = params["filename"]
		}
	}

	// Redirect the client with Content-Disposition header
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileName))
	c.Redirect(http.StatusFound, snapshotURL)
}

// filterGoodNodes filters out nodes that are not marked as "good".
func filterGoodNodes(nodes []EvaluatedNode) []EvaluatedNode {
	var goodNodes []EvaluatedNode
	for _, node := range nodes {
		if node.Status == "good" {
			goodNodes = append(goodNodes, node)
		} else {
			log.Printf("Discarding node %s (status: %s)", node.RPC, node.Status)
		}
	}
	return goodNodes
}

func setIsRefreshing(status bool) {
	refreshMutex.Lock()
	defer refreshMutex.Unlock()
	isRefreshing = status
}

func getIsRefreshing() bool {
	refreshMutex.Lock()
	defer refreshMutex.Unlock()
	return isRefreshing
}

func handleSnapshotRequest(c *gin.Context, snapshotType string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	if len(cachedNodes) == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "No available nodes. Cache is empty."})
		return
	}

	// Select the best RPC node
	bestRPC := selectBestRPC(cachedNodes)
	if bestRPC == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "No good nodes found."})
		return
	}

	url := fmt.Sprintf("%s/%s", bestRPC, snapshotType)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching snapshot from %s: %v", url, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("Failed to fetch snapshot from %s", bestRPC)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Snapshot request to %s returned status: %d", url, resp.StatusCode)
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("Snapshot node returned status: %d", resp.StatusCode)})
		return
	}

	c.DataFromReader(resp.StatusCode, resp.ContentLength, resp.Header.Get("Content-Type"), resp.Body, nil)
}

func listNodes(c *gin.Context) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	if len(cachedNodes) == 0 {
		c.JSON(http.StatusNoContent, gin.H{"message": "No good nodes found"})
		return
	}
	c.JSON(http.StatusOK, cachedNodes)
}

func getStatus(c *gin.Context) {
	status := gin.H{
		"isRefreshing": getIsRefreshing(),
		"cachedNodes":  cachedNodes,
	}
	c.JSON(http.StatusOK, status)
}

func fetchRPCNodes(endpoint string) ([]RPCNode, error) {
	payload := []byte(`{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}`)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch RPC nodes: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Result []struct {
			RPC     string `json:"rpc"`
			Version string `json:"version"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	var nodes []RPCNode
	for _, node := range result.Result {
		if node.RPC != "" {
			nodes = append(nodes, RPCNode{
				Address: node.RPC,
				Version: node.Version,
			})
		}
	}
	return nodes, nil
}

func getReferenceSlot(rpcAddress string) (int, error) {
	payload := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "getSlot",
		"params":  []interface{}{},
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", rpcAddress, bytes.NewBuffer(body))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch slot: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %v", err)
	}

	var result struct {
		Result int `json:"result"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("failed to parse response: %v", err)
	}

	return result.Result, nil
}

func selectBestRPC(results []EvaluatedNode) string {
	var bestRPC string
	var bestSpeed float64

	for _, result := range results {
		if result.Status == "good" && result.Speed > bestSpeed {
			bestSpeed = result.Speed
			bestRPC = result.RPC
		}
	}

	return bestRPC
}

// Include your `evaluateNodesWithVersions` function here
// Include your `MeasureSpeed` function here

func MeasureSpeed(url string, measureTime int) (float64, float64, error) {
	client := &http.Client{
		Timeout: 10 * time.Second, // Connection timeout
	}

	// Measure latency
	startTime := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch URL: %v", err)
	}
	defer resp.Body.Close()
	latency := time.Since(startTime).Milliseconds() // Latency in ms

	// Measure download speed
	buffer := make([]byte, 81920) // Chunk size
	var totalLoaded int64
	var speeds []float64

	lastTime := time.Now()
	for time.Since(startTime).Seconds() < float64(measureTime) {
		n, err := resp.Body.Read(buffer)
		if n > 0 {
			totalLoaded += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, float64(latency), fmt.Errorf("error reading response body: %v", err)
		}

		// Calculate speed every second
		elapsed := time.Since(lastTime).Seconds()
		if elapsed >= 1 {
			speed := float64(totalLoaded) / elapsed // Bytes/sec
			speeds = append(speeds, speed)
			lastTime = time.Now()
			totalLoaded = 0
		}
	}

	if len(speeds) == 0 {
		return 0, float64(latency), fmt.Errorf("no data collected during the measurement period")
	}

	medianSpeed := calculateMedian(speeds) / (1024 * 1024) // Convert to MB/s
	return medianSpeed, float64(latency), nil
}

func calculateMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	n := len(values)
	sort.Float64s(values)
	if n%2 == 0 {
		return (values[n/2-1] + values[n/2]) / 2
	}
	return values[n/2]
}
func evaluateNodesWithVersions(nodes []RPCNode, config Config, defaultSlot int) []EvaluatedNode {
	results := make(chan EvaluatedNode, len(nodes))

	var wg sync.WaitGroup
	sem := make(chan struct{}, config.WorkerCount) // Semaphore to control concurrency

	appendResult := func(node RPCNode, rpc string, speed, latency float64, slot, diff int, status string) {
		results <- EvaluatedNode{
			RPC:     rpc,
			Speed:   speed,
			Latency: latency,
			Slot:    slot,
			Diff:    diff,
			Version: node.Version,
			Status:  status,
		}
	}

	for _, node := range nodes {
		wg.Add(1)
		go func(node RPCNode) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire semaphore
			defer func() { <-sem }() // Release semaphore

			rpc := node.Address
			if !strings.HasPrefix(rpc, "http://") && !strings.HasPrefix(rpc, "https://") {
				rpc = "http://" + rpc
			}

			// Check snapshot availability
			baseURL, err := url.Parse(rpc)
			if err != nil {
				appendResult(node, rpc, 0, 0, 0, 0, "bad")
				return
			}
			baseURL.Path = "/snapshot.tar.bz2"
			snapshotURL := baseURL.String()

			// Measure speed and latency
			speed, latency, err := MeasureSpeed(snapshotURL, config.SleepBeforeRetry)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			// Fetch slot
			slot, err := getReferenceSlot(rpc)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			// Calculate slot difference
			diff := defaultSlot - slot
			status := "slow"
			if speed >= float64(config.MinDownloadSpeed) && latency <= float64(config.MaxLatency) && diff <= 100 {
				status = "good"
			}

			appendResult(node, rpc, speed, latency, slot, diff, status)
		}(node)
	}

	wg.Wait()
	close(results)

	// Collect results
	var evaluatedResults []EvaluatedNode
	for result := range results {
		evaluatedResults = append(evaluatedResults, result)
	}
	return evaluatedResults
}
