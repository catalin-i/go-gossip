package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log"
	"math/big"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		deltaVal := body["delta"]
		delta := deltaVal.(float64)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for {
			counter, err := kv.ReadInt(ctx, "counter")
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					counter = 0
				} else {
					return err
				}
			}
			updatedCounter := counter + int(delta)

			if err := kv.CompareAndSwap(ctx, "counter", counter, updatedCounter, true); err == nil {
				break
			}
		}

		body["type"] = "add_ok"
		delete(body, "delta")

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		randomInt, _ := rand.Int(rand.Reader, big.NewInt(100))
		kv.Write(context.Background(), "rand", randomInt)

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

		body["type"] = "read_ok"
		body["value"] = value

		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
