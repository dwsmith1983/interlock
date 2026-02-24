package firestore

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
)

// AcquireLock attempts to acquire a distributed lock with the given key and TTL.
// Uses a Firestore transaction: succeeds only if the lock doesn't exist or has expired.
func (p *FirestoreProvider) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	id := docID(lockPK(key), lockSK())
	now := time.Now().Unix()
	ttlVal := ttlEpoch(ttl)

	var acquired bool
	err := p.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		acquired = false

		snap, err := tx.Get(p.coll().Doc(id))
		if err != nil {
			if isNotFound(err) {
				// Lock doesn't exist — acquire it.
				acquired = true
				return tx.Set(p.coll().Doc(id), map[string]interface{}{
					"ttl": ttlVal,
				})
			}
			return err
		}

		// Lock exists — check if expired.
		existingTTL, _ := snapInt64(snap, "ttl")
		if existingTTL > 0 && now > existingTTL {
			// Lock expired — acquire it.
			acquired = true
			return tx.Set(p.coll().Doc(id), map[string]interface{}{
				"ttl": ttlVal,
			})
		}

		// Lock is still held.
		return nil
	})
	if err != nil {
		return false, err
	}
	return acquired, nil
}

// ReleaseLock releases a distributed lock.
func (p *FirestoreProvider) ReleaseLock(ctx context.Context, key string) error {
	id := docID(lockPK(key), lockSK())
	_, err := p.coll().Doc(id).Delete(ctx)
	return err
}
