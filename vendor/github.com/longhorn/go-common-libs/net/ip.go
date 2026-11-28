package net

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
)

const (
	EnvPodIP = "POD_IP"

	StorageNetworkInterface = "lhnet1"
)

var errInterfaceNotFound = errors.New("interface not found")

// IPFamily identifies an IP address family.
type IPFamily string

const (
	IPFamilyUnspecified IPFamily = ""
	IPFamilyIPv4        IPFamily = "ipv4"
	IPFamilyIPv6        IPFamily = "ipv6"
)

// ParseIPFamily parses an IP family case-insensitively. An empty value returns IPFamilyUnspecified.
func ParseIPFamily(value string) (IPFamily, error) {
	family := IPFamily(strings.ToLower(value))
	switch family {
	case IPFamilyUnspecified, IPFamilyIPv4, IPFamilyIPv6:
		return family, nil
	default:
		return "", fmt.Errorf("invalid IP family %q", value)
	}
}

// ParseIPFamilyFromAddress returns the family of a bare IP address or an
// IP address with a port. Hostnames and malformed addresses are rejected.
func ParseIPFamilyFromAddress(address string) (IPFamily, error) {
	ip, err := netip.ParseAddr(address)
	if err != nil {
		ipWithPort, err := netip.ParseAddrPort(address)
		if err != nil {
			return "", errors.Wrapf(err, "invalid IP address %q", address)
		}
		ip = ipWithPort.Addr()
	}
	if ip.Zone() != "" {
		return "", errors.Errorf("invalid IP address %q", address)
	}
	if ip.Unmap().Is4() {
		return IPFamilyIPv4, nil
	}
	return IPFamilyIPv6, nil
}

// getInterfaceAddrs returns the addresses for an interface.
//
// If the named interface is absent, the returned error wraps
// errInterfaceNotFound. Errors from interface enumeration, a down interface,
// or address enumeration are returned to the caller.
func getInterfaceAddrs(name string) ([]net.Addr, error) {
	return getInterfaceAddrsWithHooks(name, net.Interfaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	})
}

// getInterfaceAddrsWithHooks is the testable implementation of
// getInterfaceAddrs. It returns an errInterfaceNotFound-wrapped error when
// name is absent and returns errors from the matching interface unchanged or
// with context.
func getInterfaceAddrsWithHooks(name string,
	listInterfaces func() ([]net.Interface, error),
	listAddrs func(net.Interface) ([]net.Addr, error)) ([]net.Addr, error) {
	interfaces, err := listInterfaces()
	if err != nil {
		return nil, err
	}

	for i := range interfaces {
		if interfaces[i].Name != name {
			continue
		}
		if interfaces[i].Flags&net.FlagUp == 0 {
			return nil, errors.Errorf("interface %s is down", name)
		}

		addrs, err := listAddrs(interfaces[i])
		if err != nil {
			return nil, errors.Wrapf(err, "failed to list addresses for interface %s", name)
		}
		if len(addrs) == 0 {
			return nil, errors.Errorf("interface %s doesn't have address", name)
		}

		return addrs, nil
	}

	return nil, errors.Wrapf(errInterfaceNotFound, "interface %s not found", name)
}

func getIPFromAddr(addr net.Addr) net.IP {
	switch addr := addr.(type) {
	case *net.IPNet:
		if addr == nil {
			return nil
		}
		return addr.IP
	case *net.IPAddr:
		if addr == nil {
			return nil
		}
		return addr.IP
	default:
		return nil
	}
}

func getLocalIPFromAddrsByFamily(addrs []net.Addr, family IPFamily) string {
	for _, addr := range addrs {
		ip := getIPFromAddr(addr)
		if IsUsableIPForFamily(ip, family) {
			return ip.String()
		}
	}

	return ""
}

func getInterfaceNameByIP(ip net.IP) (string, error) {
	return getInterfaceNameByIPWithHooks(ip, net.Interfaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	})
}

func getInterfaceNameByIPWithHooks(
	ip net.IP,
	interfacesFunc func() ([]net.Interface, error),
	addrsFunc func(net.Interface) ([]net.Addr, error),
) (string, error) {
	if ip == nil {
		return "", nil
	}

	interfaces, err := interfacesFunc()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		addrs, err := addrsFunc(iface)
		if err != nil {
			return "", errors.Wrapf(err, "failed to list addresses for interface %s", iface.Name)
		}

		for _, addr := range addrs {
			addrIP := getIPFromAddr(addr)
			if addrIP.Equal(ip) {
				return iface.Name, nil
			}
		}
	}

	return "", nil
}

// GetLocalIPv4fromInterface returns the local IPv4 address.
//
// Deprecated: use GetLocalIPFromInterface instead, which also supports IPv6-only
// interfaces.
func GetLocalIPv4fromInterface(name string) (ip string, err error) {
	return getLocalIPFromInterface(name, true)
}

// GetLocalIPFromInterface returns the local IP address of the given interface.
// IPv4 is preferred, and a global unicast IPv6 address is returned when the
// interface has no IPv4 address.
func GetLocalIPFromInterface(name string) (ip string, err error) {
	return getLocalIPFromInterface(name, false)
}

func getLocalIPFromInterface(name string, ipv4Only bool) (string, error) {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", errors.Wrapf(err, "interface %s doesn't have address", name)
	}

	var ipv6 net.IP
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP == nil {
			continue
		}

		if ipv4 := ipNet.IP.To4(); ipv4 != nil {
			return ipv4.String(), nil
		}

		// Link-local and multicast addresses are not routable between nodes.
		if ipv6 == nil && ipNet.IP.IsGlobalUnicast() {
			ipv6 = ipNet.IP
		}
	}

	if ipv4Only {
		return "", errors.Errorf("interface %s don't have an IPv4 address", name)
	}
	if ipv6 == nil {
		return "", errors.Errorf("interface %s doesn't have an IPv4 or a global unicast IPv6 address", name)
	}

	return ipv6.String(), nil
}

func getIPForPod(family IPFamily, podIP string,
	interfaceAddrs func(string) ([]net.Addr, error),
	interfaceNameByIP func(net.IP) (string, error)) (string, error) {
	switch family {
	case IPFamilyUnspecified, IPFamilyIPv4, IPFamilyIPv6:
	default:
		return "", fmt.Errorf("invalid IP family %q", family)
	}

	storageAddrs, storageErr := interfaceAddrs(StorageNetworkInterface)
	if storageErr == nil {
		if ip := getLocalIPFromAddrsByFamily(storageAddrs, family); ip != "" {
			return ip, nil
		}
	} else if !errors.Is(storageErr, errInterfaceNotFound) {
		return "", storageErr
	} else {
		logrus.WithError(storageErr).Tracef("Failed to get IP from %v interface, fallback to use the default pod IP %v",
			StorageNetworkInterface, podIP)

		parsedPodIP := net.ParseIP(podIP)
		if !IsUsableIPForFamily(parsedPodIP, IPFamilyUnspecified) {
			return "", errors.Errorf("invalid %s %q", EnvPodIP, podIP)
		}

		// The primary POD_IP is authoritative when its family satisfies the
		// request, so no primary-interface lookup is needed. Only an explicit
		// request for the other family needs alternate addresses from that
		// primary interface.
		if IsUsableIPForFamily(parsedPodIP, family) {
			return parsedPodIP.String(), nil
		}

		interfaceName, err := interfaceNameByIP(parsedPodIP)
		if err != nil {
			return "", err
		}
		if interfaceName != "" {
			interfaceAddrsForPodIP, err := interfaceAddrs(interfaceName)
			if err != nil {
				return "", err
			}
			if ip := getLocalIPFromAddrsByFamily(interfaceAddrsForPodIP, family); ip != "" {
				return ip, nil
			}
		}
	}

	familyDesc := string(family)
	if family == IPFamilyUnspecified {
		familyDesc = "IP"
	}
	if storageErr == nil {
		return "", fmt.Errorf("no usable %s address found on storage network interface %s", familyDesc, StorageNetworkInterface)
	}
	return "", fmt.Errorf("no usable %s address found on pod network for pod IP %q", familyDesc, podIP)
}

// SelectIPByNetworkPreference selects the first usable global-unicast address
// from the authoritative candidate list, preserving its order.
func SelectIPByNetworkPreference(storageNetworkPresent bool, storageIPs []string, podIPs []string) (string, error) {
	source, candidates := "pod", podIPs
	if storageNetworkPresent {
		source, candidates = "storage", storageIPs
	}

	for _, candidate := range candidates {
		if ip := net.ParseIP(candidate); IsUsableIPForFamily(ip, IPFamilyUnspecified) {
			return ip.String(), nil
		}
	}

	return "", errors.Errorf("no usable global-unicast address in %s candidates %v", source, candidates)
}

// GetPreferredPodIP returns the pod IP selected from the storage network,
// falling back to the primary pod interface when the storage network is absent.
func GetPreferredPodIP() (ip string, err error) {
	return GetIPForPodByNetworkAndFamily(IPFamilyUnspecified)
}

// GetIPForPod returns the pod IP selected from the storage network, falling
// back to the primary pod interface when the storage network is absent.
//
// Deprecated: GetIPForPod has a misleading generic name and retains
// unspecified-family selection. Use GetPreferredPodIP for the same behavior,
// or GetIPForPodByNetworkAndFamily for explicit family selection.
func GetIPForPod() (ip string, err error) {
	return GetPreferredPodIP()
}

// GetIPForPodByNetworkAndFamily returns the pod IP for the requested address
// family from the storage network, or from the primary pod interface when
// the storage network is absent. It does not fall back on family mismatch.
func GetIPForPodByNetworkAndFamily(family IPFamily) (ip string, err error) {
	return getIPForPod(
		family,
		os.Getenv(EnvPodIP),
		getInterfaceAddrs,
		getInterfaceNameByIP,
	)
}

// IsUsableIPForFamily reports whether ip is a global-unicast address in the
// requested family.
func IsUsableIPForFamily(ip net.IP, family IPFamily) bool {
	if ip == nil || !ip.IsGlobalUnicast() {
		return false
	}

	switch family {
	case IPFamilyUnspecified:
		return true
	case IPFamilyIPv4:
		// IPv4 addresses also have a 16-byte representation, so To4 alone
		// determines whether the address belongs to the requested IPv4 family.
		return ip.To4() != nil
	case IPFamilyIPv6:
		// It is possible to convert IPv4 into a 16-byte expression.
		// This method expects the IP expression in the correct family.
		return ip.To4() == nil && ip.To16() != nil
	default:
		return false
	}
}

// IsLoopbackHost checks if the given host is a loopback host.
func IsLoopbackHost(host string) bool {
	if host == "localhost" || host == "127.0.0.1" || host == "0.0.0.0" || host == "::1" || host == "" {
		return true
	}
	// Check for loopback network.
	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}

	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false
		}
	}

	return true
}

// GetAnyExternalIP returns an external IP address. IPv4 is preferred, and a
// global unicast IPv6 address is returned when no IPv4 address is available.
func GetAnyExternalIP() (string, error) {
	return getAnyExternalIP(net.Interfaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	})
}

func getAnyExternalIP(
	listInterfaces func() ([]net.Interface, error),
	listAddrs func(net.Interface) ([]net.Addr, error),
) (string, error) {
	ifaces, err := listInterfaces()
	if err != nil {
		return "", err
	}

	var ipv6 net.IP

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := listAddrs(iface)
		if err != nil {
			return "", err
		}

		for _, addr := range addrs {
			ip := getIPFromAddr(addr)
			if ip == nil || ip.IsLoopback() {
				continue
			}
			if ipv4 := ip.To4(); ipv4 != nil {
				return ipv4.String(), nil
			}
			if ipv6 == nil && IsUsableIPForFamily(ip, IPFamilyIPv6) {
				ipv6 = ip
			}
		}
	}

	if ipv6 != nil {
		return ipv6.String(), nil
	}

	return "", fmt.Errorf("the current host is probably not connected to the network")
}
