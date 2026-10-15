// Package eventrelay runs the Projects transactional outbox through Platform's
// durable claim, retry, and acknowledgement protocol.
package eventrelay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/leafrun/leafrun-platform/events"
)

const (
	idlePollInterval  = 250 * time.Millisecond
	errorPollInterval = time.Second
)

// Run continuously publishes pending events and exits cleanly when ctx is
// canceled. Publish or database errors leave the outbox row available for a
// later attempt through Platform's persisted retry schedule.
func Run(ctx context.Context, db events.Beginner, publisher events.Publisher) error {
	relay := &events.Relay{DB: db, Publisher: publisher}
	for {
		published, err := relay.RunOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			slog.Error("Projects event relay attempt failed", "error_type", fmt.Sprintf("%T", err))
		}
		if err == nil && published > 0 {
			continue
		}

		wait := idlePollInterval
		if err != nil {
			wait = errorPollInterval
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
