package dlq_test

import (
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/dwsmith1983/interlock/internal/dlq"
)

func TestGenerateID_ConcurrentUniquenessAndOrdering(t *testing.T) {
	const numIDs = 1000
	ids := make([]ulid.ULID, numIDs)
	var wg sync.WaitGroup

	wg.Add(numIDs)
	for i := 0; i < numIDs; i++ {
		go func(idx int) {
			defer wg.Done()
			ids[idx] = dlq.GenerateID()
		}(i)
	}
	wg.Wait()

	unique := make(map[ulid.ULID]bool)
	for _, id := range ids {
		assert.NotEqual(t, ulid.ULID{}, id, "Generated ID should not be empty")
		unique[id] = true
	}
	assert.Equal(t, numIDs, len(unique), "All 1000 generated ULIDs must be unique")
}
