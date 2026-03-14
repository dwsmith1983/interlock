package trigger

import (
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"
)

// newSSRFSafeTransport clones http.DefaultTransport (preserving HTTP/2,
// keep-alive, idle-conn settings) and replaces the dialer with one whose
// Control hook rejects connections to private/loopback/link-local IPs.
func newSSRFSafeTransport() *http.Transport {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.DialContext = (&net.Dialer{
		Timeout:   base.TLSHandshakeTimeout, // match the original dialer timeout
		KeepAlive: 30 * time.Second,
		Control:   ssrfDialControl,
	}).DialContext
	return base
}

func ssrfDialControl(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("ssrf: invalid address %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("ssrf: could not parse IP %q", host)
	}
	if isBlockedIP(ip) {
		return fmt.Errorf("ssrf: connection to %s blocked (private/loopback/link-local)", ip)
	}
	return nil
}

func isBlockedIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified() ||
		// Explicit IMDS/ECS checks — already covered by IsLinkLocalUnicast
		// but kept for visibility since these are the primary SSRF targets.
		ip.Equal(net.ParseIP("169.254.169.254")) ||
		ip.Equal(net.ParseIP("169.254.170.2"))
}
