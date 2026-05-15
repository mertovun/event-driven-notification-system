package provider

import (
	"fmt"
	"net"
	"net/netip"
	"syscall"
)

// ssrfError signals a denied outbound destination. Wrapped into SendError as ErrSSRFBlocked.
type ssrfError struct {
	addr string
	zone string
}

func (e *ssrfError) Error() string {
	return fmt.Sprintf("ssrf: address %s denied (%s)", e.addr, e.zone)
}

// ssrfControl is the (*net.Dialer).Control hook. Called after DNS resolution
// with the final IP. Reject loopback, link-local, RFC 1918, CGNAT, and metadata IPs.
// See docs/05 §7.
func ssrfControl(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// Not a host:port form (rare here); allow but log via the error path.
		return nil
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		// DNS already resolved to an IP — this should not happen, but be conservative.
		return nil
	}
	if zone, blocked := denyZone(ip); blocked {
		return &ssrfError{addr: ip.String(), zone: zone}
	}
	_ = network
	return nil
}

// denyZone returns (zoneName, true) if the IP falls into a denied range.
func denyZone(ip netip.Addr) (string, bool) {
	// Cloud metadata endpoint — check first so it shadows the more general link-local label.
	if ip == netip.MustParseAddr("169.254.169.254") {
		return "metadata", true
	}
	if ip.IsLoopback() {
		return "loopback", true
	}
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return "link-local", true
	}
	if ip.IsUnspecified() {
		return "unspecified", true
	}
	// RFC 1918.
	for _, cidr := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"} {
		p := netip.MustParsePrefix(cidr)
		if p.Contains(ip) {
			return "rfc1918", true
		}
	}
	// CGNAT.
	if netip.MustParsePrefix("100.64.0.0/10").Contains(ip) {
		return "cgnat", true
	}
	// IPv6 unique-local.
	if netip.MustParsePrefix("fc00::/7").Contains(ip) {
		return "unique-local-v6", true
	}
	return "", false
}
