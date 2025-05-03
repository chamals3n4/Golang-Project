package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"mini_etcd/config"
)

type RPC string

const (
	RPCRequestVote   RPC = "RequestVote"
	RPCAppendEntries RPC = "AppendEntries"
)

type Handler func(method RPC, body io.Reader, w http.ResponseWriter)

type HTTPTransport struct {
	handler Handler
	client  *http.Client
}

func New(handler Handler) *HTTPTransport {
	return &HTTPTransport{
		handler: handler,
		client: &http.Client{
			Timeout: config.RPCTimeout,
			Transport: &http.Transport{
				ResponseHeaderTimeout: config.RPCTimeout,
				IdleConnTimeout:      config.RPCTimeout * 2,
				MaxIdleConnsPerHost: 100,
			},
		},
	}
}

func (t *HTTPTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	method := RPC(r.URL.Path[1:])
	t.handler(method, r.Body, w)
}

func (t *HTTPTransport) Call(addr string, method RPC, args interface{}, reply interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(args); err != nil {
		return fmt.Errorf("encode: %v", err)
	}

	url := fmt.Sprintf("http://%s/%s", addr, string(method))
	resp, err := t.client.Post(url, "application/json", &buf)
	if err != nil {
		return fmt.Errorf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status: %s", resp.Status)
	}

	if err := json.NewDecoder(resp.Body).Decode(reply); err != nil {
		return fmt.Errorf("decode: %v", err)
	}
	return nil
}

// Utility to reply JSON.
func ReplyJSON(w http.ResponseWriter, v interface{}) {
	data, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
