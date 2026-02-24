// Package firestore implements the Provider interface using Google Cloud Firestore Native Mode.
package firestore

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Compile-time interface satisfaction check.
var _ provider.Provider = (*FirestoreProvider)(nil)

// Storage defaults.
const (
	defaultCollection  = "interlock"
	defaultReadinessTTL = 5 * time.Minute
	defaultRetentionTTL = 7 * 24 * time.Hour // 7 days
)

// FirestoreProvider implements the Provider interface backed by Firestore Native Mode.
type FirestoreProvider struct {
	client       *firestore.Client
	collection   string
	logger       *slog.Logger
	readinessTTL time.Duration
	retentionTTL time.Duration
}

// New creates a new FirestoreProvider.
func New(cfg *types.FirestoreConfig) (*FirestoreProvider, error) {
	if cfg == nil {
		return nil, fmt.Errorf("firestore config is required")
	}
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("firestore projectId is required")
	}

	// Support the Firestore emulator via FIRESTORE_EMULATOR_HOST or config.
	if cfg.Emulator != "" {
		os.Setenv("FIRESTORE_EMULATOR_HOST", cfg.Emulator)
	}

	client, err := firestore.NewClient(context.Background(), cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("creating Firestore client: %w", err)
	}

	collection := cfg.Collection
	if collection == "" {
		collection = defaultCollection
	}

	readinessTTL := defaultReadinessTTL
	if cfg.ReadinessTTL != "" {
		if d, err := time.ParseDuration(cfg.ReadinessTTL); err == nil && d > 0 {
			readinessTTL = d
		}
	}
	retentionTTL := defaultRetentionTTL
	if cfg.RetentionTTL != "" {
		if d, err := time.ParseDuration(cfg.RetentionTTL); err == nil && d > 0 {
			retentionTTL = d
		}
	}

	return &FirestoreProvider{
		client:       client,
		collection:   collection,
		logger:       slog.Default(),
		readinessTTL: readinessTTL,
		retentionTTL: retentionTTL,
	}, nil
}

// coll returns the primary collection reference.
func (p *FirestoreProvider) coll() *firestore.CollectionRef {
	return p.client.Collection(p.collection)
}

// Start initializes the provider.
func (p *FirestoreProvider) Start(_ context.Context) error {
	return nil
}

// Stop closes the Firestore client.
func (p *FirestoreProvider) Stop(_ context.Context) error {
	return p.client.Close()
}

// Ping checks connectivity by reading a non-existent document.
func (p *FirestoreProvider) Ping(ctx context.Context) error {
	_, err := p.coll().Doc("__ping__").Get(ctx)
	// NotFound is fine — it means connectivity works.
	if isNotFound(err) {
		return nil
	}
	return err
}
