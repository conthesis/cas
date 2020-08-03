package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
	"log"
	url "net/url"
	"os"
	"time"
)

const casGetTopic = "conthesis.cas.get"
const casStoreTopic = "conthesis.cas.store"

func getRequiredEnv(env string) (string, error) {
	val := os.Getenv(env)
	if val == "" {
		return "", fmt.Errorf("`%s`, a required environment variable was not set", env)
	}
	return val, nil
}

func NewNats(lc fx.Lifecycle) (*nats.Conn, error) {
	natsURL, err := getRequiredEnv("NATS_URL")
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(natsURL)

	if err != nil {
		if err, ok := err.(*url.Error); ok {
			return nil, fmt.Errorf("NATS_URL is of an incorrect format: %w", err)
		}
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return nc.Drain()
		},
	})

	return nc, nil
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
		return
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

func setupSubscriptions(gc *gcas) error {
	_, err := gc.nc.Subscribe(casStoreTopic, gc.storeHandler)
	if err != nil {
		return err
	}
	_, err = gc.nc.Subscribe(casGetTopic, gc.getHandler)
	if err != nil {
		return err
	}
	return nil
}

func NewGCas(nc *nats.Conn, storage Storage) *gcas {
	return &gcas{nc, storage}
}

func main() {
	app := fx.New(
		fx.Provide(
			NewNats,
			NewStorage,
			NewGCas,
		),
		fx.Invoke(setupSubscriptions),
	)
	startCtx, cancel := context.WithTimeout(context.Background(), app.StartTimeout())
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		log.Fatal(err)
	}

	<-app.Done()

	stopCtx, cancel := context.WithTimeout(context.Background(), app.StopTimeout())
	defer cancel()
	if err := app.Stop(stopCtx); err != nil {
		log.Fatal(err)
	}
}
