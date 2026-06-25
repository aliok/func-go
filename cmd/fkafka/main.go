package main

import (
	"context"
	"fmt"
	"os"

	"knative.dev/func-go/kafka"
)

// Main illustrates how scaffolding works to wrap a user's function.
func main() {
	// Instanced Example
	// (in scaffolding 'New()' will be in module 'f')
	if err := kafka.Start(New()); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	// Static Example
	// (in scaffolding 'Handle' will be in the module 'f')
	// if err := kafka.Start(kafka.DefaultHandler{Handler: Handle}); err != nil {
	//   fmt.Fprintln(os.Stderr, err.Error())
	//   os.Exit(1)
	// }
}

// Handle is an example static function implementation.
func Handle(ctx context.Context, msg kafka.Message) error {
	fmt.Println("Static Kafka Handler invoked")
	return nil
}

// MyFunction is an example instanced Kafka function implementation.
type MyFunction struct{}

func New() *MyFunction {
	return &MyFunction{}
}

func (f *MyFunction) Handle(ctx context.Context, msg kafka.Message) error {
	fmt.Printf("Received message: topic=%s partition=%d offset=%d key=%s value=%s\n",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	return nil
}
