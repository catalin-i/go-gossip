# Go gossip protocols exercises

These are solutions to Fly.io's distributed systems challenges 4 to 6: https://fly.io/dist-sys/

For challenges 1 to 3, check out my solutions in Rust: https://github.com/catalin-i/rust-gossip

## Prerequisites

1. Install [Maelstrom](https://github.com/jepsen-io/maelstrom)
2. Install the [go toolchain](https://go.dev/doc/install)
3. Set **MAELSTROM_LOC** to where your Maelstrom binary is located

## To run a specific challenge implementation

1. run `go get github.com/jepsen-io/maelstrom/demo/go` in the specific challenge subdirectory
2. Run `go install .` in the specific challenge subdirectory
3. Run the corresponding challenge script file
