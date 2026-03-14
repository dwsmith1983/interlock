package trigger

import (
	"net"
	"testing"
)

func TestIsBlockedIP(t *testing.T) {
	blocked := []struct {
		name string
		ip   string
	}{
		{"loopback_v4", "127.0.0.1"},
		{"private_10", "10.0.0.1"},
		{"private_172", "172.16.0.1"},
		{"private_192", "192.168.1.1"},
		{"aws_imds", "169.254.169.254"},
		{"ecs_metadata", "169.254.170.2"},
		{"loopback_v6", "::1"},
		{"link_local_v6", "fe80::1"},
		{"unspecified", "0.0.0.0"},
	}
	for _, tc := range blocked {
		t.Run(tc.name, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("failed to parse IP %s", tc.ip)
			}
			if !isBlockedIP(ip) {
				t.Errorf("expected %s to be blocked", tc.ip)
			}
		})
	}

	allowed := []struct {
		name string
		ip   string
	}{
		{"google_dns", "8.8.8.8"},
		{"aws_public", "52.94.76.1"},
		{"google_v6", "2607:f8b0:4004:800::200e"},
	}
	for _, tc := range allowed {
		t.Run(tc.name, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("failed to parse IP %s", tc.ip)
			}
			if isBlockedIP(ip) {
				t.Errorf("expected %s to be allowed", tc.ip)
			}
		})
	}
}

func TestSSRFDialControl(t *testing.T) {
	t.Run("blocks_loopback", func(t *testing.T) {
		err := ssrfDialControl("tcp", "127.0.0.1:80", nil)
		if err == nil {
			t.Error("expected error for loopback address")
		}
	})

	t.Run("allows_public", func(t *testing.T) {
		err := ssrfDialControl("tcp", "8.8.8.8:443", nil)
		if err != nil {
			t.Errorf("expected no error for public address, got: %v", err)
		}
	})

	t.Run("blocks_imds", func(t *testing.T) {
		err := ssrfDialControl("tcp", "169.254.169.254:80", nil)
		if err == nil {
			t.Error("expected error for IMDS address")
		}
	})
}
