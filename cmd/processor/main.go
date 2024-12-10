//go:build wasm

package main

import (
	processor "example.com/conduit-processor-embeddings"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

func main() {
	sdk.Run(processor.NewProcessor())
}
