package main

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidislock"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	logger := log.
		Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		With().Caller().Logger().
		Level(zerolog.TraceLevel)
	zerolog.DefaultContextLogger = &logger

	pool, err := dockertest.NewPool("")
	if err != nil {
		panic(err)
	}
	err = pool.Client.Ping()
	if err != nil {
		panic(err)
	}
	redis, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "redis",
		Tag:        "7-alpine",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"6379": {{
				HostIP:   "0.0.0.0",
				HostPort: "6379",
			}},
		},
		ExposedPorts: []string{"6379"},
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		err = pool.Purge(redis)
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second)

	const count = 2
	stopC := make(chan struct{})
	doneC := make(chan struct{})

	for i := 0; i < count; i++ {
		go work(stopC, doneC)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	signalName := (<-interrupt).String()
	logger.Debug().Str("signal", signalName).Msg("received os signal")

	go func() {
		for i := 0; i < count; i++ {
			stopC <- struct{}{}
		}
	}()
	already := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		select {
		case <-doneC:
			already++
			if already == count {
				return
			}
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
}

func work(stopC, doneC chan struct{}) {
	locker, err := rueidislock.NewLocker(rueidislock.LockerOption{
		ClientOption:   rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		KeyMajority:    2,
		KeyValidity:    time.Second * 10,
		ExtendInterval: time.Second * 2,
	})
	if err != nil {
		panic(err)
	}
	defer locker.Close()

	id := rand.Int63()
	timer := time.NewTimer(time.Millisecond)
	logger := zerolog.DefaultContextLogger.With().Int64("id", id).Logger()

	for {
		select {
		case <-timer.C:
			err := tick(locker, id)
			if errors.Is(err, rueidislock.ErrNotLocked) {
				logger.Debug().Msg("already acquired")
				timer.Reset(time.Millisecond * 500)
				continue
			}
			if err != nil {
				panic(err)
			}
			timer.Reset(time.Second)
		case <-stopC:
			doneC <- struct{}{}
			return
		}
	}
}

func tick(locker rueidislock.Locker, id int64) error {
	logger := zerolog.DefaultContextLogger.With().Int64("id", id).Logger()

	logger.Debug().Msg("try to acquire")
	_, cancel, err := locker.TryWithContext(context.Background(), "my_lock")
	if err != nil {
		return err
	}
	defer func() {
		logger.Debug().Msg("try to make free")
		cancel()
		logger.Debug().Msg("made free")
	}()
	logger.Debug().Msg("successfully acquired")

	time.Sleep(time.Second * 10)

	return nil
}
