package main

import (
	"context"
	"encoding/hex"
	"github.com/nats-io/nats.go"
	"log"
	url "net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const casGetTopic = "conthesis.cas.get"
const casStoreTopic = "conthesis.cas.store"

func getRequiredEnv(env string) string {
	val := os.Getenv(env)
	if val == "" {
		log.Fatalf("`%s`, a required environment variable was not set", env)
	}
	return val
}

func connectNats() *nats.Conn {
	natsURL := getRequiredEnv("NATS_URL")
	nc, err := nats.Connect(natsURL)

	if err != nil {
		if err, ok := err.(*url.Error); ok {
			log.Fatalf("NATS_URL is of an incorrect format: %s", err.Error())
		}
		log.Fatalf("Failed to connect to NATS %T: %s", err, err)
	}
	return nc
}

type gcas struct {
	nc      *nats.Conn
	storage Storage
}

func (g *gcas) getHandler(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if len(m.Data) > 32 {
		log.Printf("Provided pointer too long: ptr = %s", m.Data)
		m.Respond([]byte(""))
		return
	}
	data, err := g.storage.Get(ctx, m.Data)
	if err != nil {
		log.Printf("Error fetching pointer %s, err: %s", hex.EncodeToString(m.Data), err)
		m.Respond([]byte(""))
	}
	m.Respond(data)
}

func (g *gcas) storeHandler(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	norm := Normalize(m.Data)
	hs := Hash(norm)
	err := g.storage.Store(ctx, hs, norm)
	if err != nil {
		log.Printf("Error storing data %s", err)
		m.Respond([]byte(""))
		return
	}
	m.Respond(hs)
}

func waitForTerm() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()
	<-done
}

func (gc *gcas) setupSubscriptions() {
	_, err := gc.nc.Subscribe(casStoreTopic, gc.storeHandler)
	if err != nil {
		log.Fatalf("Unable to subscribe to topic %s: %s", casGetTopic, err)
	}
	_, err = gc.nc.Subscribe(casGetTopic, gc.getHandler)
	if err != nil {
		log.Fatalf("Unable to subscribe to topic %s: %s", casStoreTopic, err)
	}

}

func (gc *gcas) Close() {
	log.Printf("Shutting down...")
	gc.nc.Drain()
	gc.storage.Close()
}

func main() {
	nc := connectNats()
	storage, err := newStorage()

	if err != nil {
		log.Fatalf("Failed to create storage driver: %s", err)
	}
	gc := gcas{nc: nc, storage: storage}
	defer gc.Close()
	gc.setupSubscriptions()
	log.Printf("Connected to NATS")
	waitForTerm()
}
