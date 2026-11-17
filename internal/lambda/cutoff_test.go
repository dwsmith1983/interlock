package lambda_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
)

// TestIsRerun: a RERUN# row for the pipeline, schedule and date, from either
// writer, makes the date a rerun; a row for another date or schedule does not.
func TestIsRerun(t *testing.T) {
	const date = "2026-03-10"
	ctx := context.Background()
	tests := []struct {
		name    string
		setup   func(*store.Store) error
		failQry bool
		want    bool
	}{
		{name: "NoRows", setup: func(*store.Store) error { return nil }},
		{name: "WriteRerunRow", want: true, setup: func(s *store.Store) error {
			_, err := s.WriteRerun(ctx, "p", "s", date, "manual", "")
			return err
		}},
		{name: "WriteRerunOnceRow", want: true, setup: func(s *store.Store) error {
			_, err := s.WriteRerunOnce(ctx, "p", "s", date, "p-s-2026-03-10-1-r1", "job-fail-retry", "fail")
			return err
		}},
		{name: "PreviousDateOnly", setup: func(s *store.Store) error {
			_, err := s.WriteRerun(ctx, "p", "s", "2026-03-09", "manual", "")
			return err
		}},
		{name: "OtherSchedule", setup: func(s *store.Store) error {
			_, err := s.WriteRerun(ctx, "p", "other", date, "manual", "")
			return err
		}},
		{name: "QueryError", failQry: true, setup: func(*store.Store) error { return nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, _ := storetest.NewMem()
			d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			if err := tt.setup(d.Store); err != nil {
				t.Fatalf("setup: %v", err)
			}
			if tt.failQry {
				fake.QueryFn = func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
					return nil, errors.New("dynamodb: throttled")
				}
			}

			got, err := lambda.IsRerun(ctx, d, "p", "s", date)

			if tt.failQry {
				if err == nil || !strings.Contains(err.Error(), "dynamodb: throttled") {
					t.Fatalf("IsRerun = %v, %v, want the query error", got, err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("IsRerun = %v, %v, want %v", got, err, tt.want)
			}
		})
	}
}
