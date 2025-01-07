package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type OffsetReq struct {
	Type      string             `json:"type"`
	Offsets   map[string]float64 `json:"offsets"`
	MessageId int                `json:"msg_id"`
}

type KeysReq struct {
	Type      string   `json:"type"`
	Keys      []string `json:"keys"`
	MessageId int      `json:"msg_id"`
}

type OffsetLog []int

type PollRes struct {
	Type      string                 `json:"type"`
	Messages  map[string][]OffsetLog `json:"msgs"`
	MessageId int                    `json:"msg_id"`
}

type State struct {
	Node    *maelstrom.Node
	Mutex   sync.RWMutex
	Logs    map[string][]OffsetLog
	Offsets map[string]int
}

func main() {
	n := maelstrom.NewNode()
	s := &State{
		Node:    n,
		Logs:    make(map[string][]OffsetLog),
		Offsets: make(map[string]int),
	}

	const offset_base = 1000

	n.Handle("send", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		message := body["msg"].(float64)

		s.Mutex.Lock()

		len := len(s.Logs[key]) + offset_base
		s.Logs[key] = append(s.Logs[key], OffsetLog{len, int(message)})

		s.Mutex.Unlock()

		body["type"] = "send_ok"
		body["offset"] = len
		delete(body, "msg")
		delete(body, "key")

		return n.Reply(msg, body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {

		var body OffsetReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := make(map[string][]OffsetLog)

		s.Mutex.Lock()

		if body.Offsets != nil {
			for key, value := range body.Offsets {
				logs_for_key := s.Logs[key]
				if len(logs_for_key) != 0 {
					filtered_logs := []OffsetLog{}

					start := int(value) - offset_base
					if start < 0 {
						start = 0
					}
					for i := start; i < len(logs_for_key); i++ {
						filtered_logs = append(filtered_logs, logs_for_key[i])
					}
					messages[key] = filtered_logs
				}
			}
		} else {
			for key, value := range s.Logs {
				messages[key] = value
			}
		}
		s.Mutex.Unlock()

		res := PollRes{
			Type:      "poll_ok",
			Messages:  messages,
			MessageId: body.MessageId,
		}
		return n.Reply(msg, res)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body OffsetReq

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		req_offsets := body.Offsets
		s.Mutex.Lock()
		for key, value := range req_offsets {
			entry := s.Offsets[key]
			if entry != 0 {
				if int(value) < entry {
					continue
				}
			} else {
				s.Offsets[key] = int(value)
			}
		}
		s.Mutex.Unlock()
		res := make(map[string]any)
		res["type"] = "commit_offsets_ok"
		res["msg_id"] = body.MessageId

		return n.Reply(msg, res)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body KeysReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body.Keys
		res_offsets := make(map[string]int)

		s.Mutex.RLock()
		for _, key := range keys {
			entry := s.Offsets[key]
			if entry != 0 {
				res_offsets[key] = entry
			}

		}
		s.Mutex.RUnlock()
		result := make(map[string]any)
		result["offsets"] = res_offsets
		result["type"] = "list_committed_offsets_ok"
		result["msg_id"] = body.MessageId

		return n.Reply(msg, result)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
