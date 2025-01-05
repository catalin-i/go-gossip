package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	// Register a handler for the "echo" message that responds with an "echo_ok".
	n.Handle("add", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		deltaVal := body["delta"]
		delta := deltaVal.(float64)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var rpc_err *maelstrom.RPCError
		previous, err := kv.ReadInt(ctx, "counter")
		if err != nil {
			if errors.As(err, &rpc_err) {
				if rpc_err.Code == maelstrom.KeyDoesNotExist {
					kv.CompareAndSwap(ctx, "counter", 0, 0, true)
					previous = 0
				}
			} else {
				return err
			}
		}

		to_push := previous + int(delta)
		update_err := kv.CompareAndSwap(ctx, "counter", previous, to_push, false)
		if update_err != nil {
			return update_err
		}

		// Update the message type.
		body["type"] = "add_ok"
		delete(body, "delta")

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var rpc_err *maelstrom.RPCError
		value, err := kv.ReadInt(ctx, "counter")
		if err != nil {
			if errors.As(err, &rpc_err) {
				if rpc_err.Code == maelstrom.KeyDoesNotExist {
					kv.CompareAndSwap(ctx, "counter", 0, 0, true)
					value = 0
				}
			} else {
				return err
			}
		}

		// Update the message type.
		body["type"] = "read_ok"
		body["value"] = value

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
