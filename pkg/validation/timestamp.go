package validation

import (
	"math"
	"strconv"
	"time"
)

// epochMillisFloor splits epoch seconds from epoch milliseconds: a smaller
// value is seconds (up to the year 33658), a larger one milliseconds (from
// September 2001).
const epochMillisFloor = 1e12

// ParseTimestamp reads a sensor timestamp. A string is tried as RFC3339, then
// as a base-10 integer. A number or integer string is epoch seconds below 1e12
// and epoch milliseconds otherwise. Zero, negative, NaN, infinite and any other
// type report false.
func ParseTimestamp(v interface{}) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339, x); err == nil {
			return t, true
		}
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		return fromEpoch(n)
	case float64:
		return fromEpochFloat(x)
	case float32:
		return fromEpochFloat(float64(x))
	case int:
		return fromEpoch(int64(x))
	case int32:
		return fromEpoch(int64(x))
	case int64:
		return fromEpoch(x)
	default:
		return time.Time{}, false
	}
}

// fromEpochFloat truncates f to whole units. NaN, infinities and values int64
// cannot hold report false.
func fromEpochFloat(f float64) (time.Time, bool) {
	if math.IsNaN(f) || f <= 0 || f >= math.MaxInt64 {
		return time.Time{}, false
	}
	return fromEpoch(int64(f))
}

func fromEpoch(n int64) (time.Time, bool) {
	switch {
	case n <= 0:
		return time.Time{}, false
	case n < epochMillisFloor:
		return time.Unix(n, 0), true
	default:
		return time.UnixMilli(n), true
	}
}
