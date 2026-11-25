package alert

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetryDelaySeconds(t *testing.T) {
	tests := []struct {
		receiveCount int
		retryAfter   int
		jitter       float64
		want         int32
	}{
		{receiveCount: 1, want: 60},
		{receiveCount: 2, want: 120},
		{receiveCount: 3, want: 240},
		{receiveCount: 4, want: 480},
		{receiveCount: 5, want: 900},
		{receiveCount: 10, want: 900},
		{receiveCount: 0, want: 60},
		{receiveCount: math.MaxInt, want: 900},
		{receiveCount: 1, jitter: 0.5, want: 75},
		{receiveCount: 1, retryAfter: 300, want: 300},
		{receiveCount: 1, retryAfter: 300, jitter: 0.5, want: 315},
		{receiveCount: 1, retryAfter: 7200, want: 3600},
		{receiveCount: 5, retryAfter: 7200, jitter: 0.5, want: 3825},
		{receiveCount: 5, retryAfter: 30, want: 900},
		{receiveCount: 5, jitter: math.Nextafter(1, 0), want: 1349},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.receiveCount, tt.retryAfter, tt.jitter), func(t *testing.T) {
			assert.Equal(t, tt.want, retryDelaySeconds(tt.receiveCount, tt.retryAfter, tt.jitter))
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		header string
		want   int
	}{
		{"30", 30},
		{" 30 ", 30},
		{"7200", 7200},
		{"", 0},
		{"abc", 0},
		{"-5", 0},
		{"0", 0},
		{"1.5", 0},
		{"99999999999999999999", 0},
		{"Wed, 21 Oct 2026 07:28:00 GMT", 0},
	}
	for _, tt := range tests {
		t.Run(strconv.Quote(tt.header), func(t *testing.T) {
			assert.Equal(t, tt.want, parseRetryAfter(tt.header))
		})
	}
}
