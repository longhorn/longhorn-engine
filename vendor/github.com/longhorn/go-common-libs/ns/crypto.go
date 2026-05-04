package ns

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-common-libs/utils"
)

// LuksFormatOptions defines optional parameters used when running cryptsetup luksFormat.
type LuksFormatOptions struct {
	KeyCipher            string
	KeyHash              string
	KeySize              string
	PBKDF                string
	PBKDFForceIterations string // optional. PBKDF iteration count to force when deriving the key
	PBKDFMemory          string // optional. Memory cost for PBKDF in KiB
}

// LuksOpen runs cryptsetup luksOpen with the given passphrase and
// returns the stdout and error.
func (nsexec *Executor) LuksOpen(volume, devicePath, passphrase string, timeout time.Duration) (stdout string, err error) {
	args := []string{"luksOpen", devicePath, volume, "-d", "-"}
	return nsexec.CryptsetupWithPassphrase(passphrase, args, timeout)
}

// LuksClose runs cryptsetup luksClose and returns the stdout and error.
func (nsexec *Executor) LuksClose(volume string, timeout time.Duration) (stdout string, err error) {
	args := []string{"luksClose", volume}
	return nsexec.Cryptsetup(args, timeout)
}

// LuksFormat runs `cryptsetup luksFormat` on the specified device using the
// provided passphrase and optional LuksFormatOptions (cipher, hash, key size, PBKDF settings, etc.).
func (nsexec *Executor) LuksFormat(devicePath, passphrase string, options *LuksFormatOptions, timeout time.Duration) (stdout string, err error) {
	args := []string{
		"-q", "luksFormat",
		"--type", "luks2",
	}
	if options != nil {
		if options.KeyCipher != "" {
			args = append(args, "--cipher", options.KeyCipher)
		}
		if options.KeyHash != "" {
			args = append(args, "--hash", options.KeyHash)
		}
		if options.KeySize != "" {
			args = append(args, "--key-size", options.KeySize)
		}
		if options.PBKDF != "" {
			args = append(args, "--pbkdf", options.PBKDF)
		}
		if options.PBKDFForceIterations != "" {
			args = append(args, "--pbkdf-force-iterations", options.PBKDFForceIterations)
		}
		if options.PBKDFMemory != "" {
			args = append(args, "--pbkdf-memory", options.PBKDFMemory)
		}
	}

	args = append(args, devicePath, "-d", "-")
	return nsexec.CryptsetupWithPassphrase(passphrase, args, timeout)
}

// LuksResize runs cryptsetup resize with the given passphrase and
// returns the stdout and error.
func (nsexec *Executor) LuksResize(volume, passphrase string, timeout time.Duration) (stdout string, err error) {
	args := []string{"resize", volume}
	return nsexec.CryptsetupWithPassphrase(passphrase, args, timeout)
}

// LuksStatus runs cryptsetup status and returns the stdout and error.
func (nsexec *Executor) LuksStatus(volume string, timeout time.Duration) (stdout string, err error) {
	args := []string{"status", volume}
	return nsexec.Cryptsetup(args, timeout)
}

// IsLuks checks if the device is encrypted with LUKS.
func (nsexec *Executor) IsLuks(devicePath string, timeout time.Duration) (bool, error) {
	args := []string{"isLuks", devicePath}
	_, err := nsexec.Cryptsetup(args, timeout)
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if exitErr.ExitCode() == 1 {
			// The device is not encrypted if exit code of 1 is returned
			// Ref https://gitlab.com/cryptsetup/cryptsetup/-/blob/main/FAQ.md?plain=1#L2848
			return false, nil
		}
	}
	return false, err
}

func (nsexec *Executor) GetLuksBackendSize(size int64, encrypted bool, cliAPIVersion int) (int64, error) {
	if !encrypted {
		return size, nil
	}

	is16MiBHeaderPkgVersion, err := nsexec.IsLuksFixed16MiBHeaderSize()
	if err != nil {
		return 0, errors.Wrap(err, "failed to determine if cryptsetup version has fixed 16 MiB header size")
	}
	if !is16MiBHeaderPkgVersion {
		return size, nil
	}

	return types.GetBackendSize(size, encrypted, cliAPIVersion), nil
}

func (nsexec *Executor) IsLuksFixed16MiBHeaderSize() (bool, error) {
	ver, err := nsexec.getCryptsetupVersion()
	if err != nil {
		return false, err
	}

	// The fixed header size 16 MiB was introduced in cryptsetup 2.1.0. See: https://www.kernel.org/pub/linux/utils/cryptsetup/v2.1/v2.1.0-ReleaseNotes.
	return utils.IsVersionAtLeast(ver, "2.1.0")
}

func (nsexec *Executor) getCryptsetupVersion() (string, error) {
	args := []string{"--version"}
	result, err := nsexec.Cryptsetup(args, time.Minute)
	if err != nil {
		return "", errors.Wrap(err, "cannot find cryptsetup version info on host")
	}

	//command: cryptsetup --version; result: cryptsetup 2.4.3\n
	fields := strings.Fields(result)
	if len(fields) < 2 {
		return "", fmt.Errorf("failed to parse cryptsetup version from output %q", result)
	}
	for _, field := range fields {
		if utils.IsVersionValid(field) {
			return field, nil
		}
	}
	return "", fmt.Errorf("failed to get valid cryptsetup version from %q", result)
}

// Cryptsetup runs cryptsetup without passphrase. It will return
// 0 on success and a non-zero value on error.
func (nsexec *Executor) Cryptsetup(args []string, timeout time.Duration) (stdout string, err error) {
	return nsexec.CryptsetupWithPassphrase("", args, timeout)
}

// CryptsetupWithPassphrase runs cryptsetup with passphrase. It will return
// 0 on success and a non-zero value on error.
// 1 wrong parameters, 2 no permission (bad passphrase),
// 3 out of memory, 4 wrong device specified,
// 5 device already exists or device is busy.
func (nsexec *Executor) CryptsetupWithPassphrase(passphrase string, args []string, timeout time.Duration) (stdout string, err error) {
	// NOTE: When using cryptsetup, ensure it is run in the host IPC/MNT namespace.
	// If only the MNT namespace is used, the binary will not return, but the
	// appropriate action will still be performed.
	// For Talos Linux, cryptsetup comes pre-installed in the host namespace
	// (ref: https://github.com/siderolabs/pkgs/blob/release-1.4/reproducibility/pkg.yaml#L10)
	// for the [Disk Encryption](https://www.talos.dev/v1.4/talos-guides/configuration/disk-encryption/).
	return nsexec.ExecuteWithStdin(nil, types.BinaryCryptsetup, args, passphrase, timeout)
}
