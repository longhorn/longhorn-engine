package backupstore

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/sio"

	"github.com/longhorn/backupstore/types"
)

const (
	// EncryptionFormatVersion is the persistent format version of client-side backup encryption.
	// It is a compatibility contract: a reader rejects any version newer than it understands.
	EncryptionFormatVersion = 1

	ENCRYPTION_DIRECTORY = "encryption"
	ENCRYPTION_KEY_FILE  = "key.json"

	encryptionAlgorithm        = "AES-256-GCM-DARE"
	encryptionKeyProvider      = "secret"
	encryptionKeyWrapAlgorithm = "AES-256-GCM"
	encryptionKeySize          = 32

	keyFingerprintContext = "longhorn-backup-encryption-key-fingerprint"
)

// ErrBackupDecryptionFailed is returned when ciphertext or authenticated metadata fails
// verification. It is never retried and never falls back to reading the data as plaintext.
var ErrBackupDecryptionFailed = errors.New("failed to decrypt backup: authentication failed")

// EncryptionConfig is the request-scoped encryption input: the Secret name and its
// key-encryption key (KEK). The KEK is unexported so it is never serialized.
type EncryptionConfig struct {
	SecretName string
	kek        []byte
}

// String keeps the KEK out of any formatted output, including %v and %+v.
func (c *EncryptionConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("EncryptionConfig{SecretName: %q}", c.SecretName)
}

// GoString keeps the KEK out of %#v output.
func (c *EncryptionConfig) GoString() string {
	return c.String()
}

// NewEncryptionConfig builds the encryption input from the request credential map. It
// returns nil when no encryption Secret is set. Errors name the Secret, never the key.
func NewEncryptionConfig(credential map[string]string) (*EncryptionConfig, error) {
	secretName := credential[types.BackupEncryptionSecret]
	encodedKey := strings.TrimSpace(credential[types.BackupEncryptionKey])
	if secretName == "" && encodedKey == "" {
		return nil, nil
	}
	if secretName == "" {
		return nil, errors.New("backup encryption key is set without a backup encryption secret name")
	}
	kek, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(kek) != encryptionKeySize {
		return nil, fmt.Errorf("invalid backup encryption key in secret %q", secretName)
	}
	return &EncryptionConfig{SecretName: secretName, kek: kek}, nil
}

// BackupEncryption is the plaintext encryption marker stored in volume.cfg and
// backup_<name>.cfg. It holds only the format version and the Secret name.
type BackupEncryption struct {
	FormatVersion int    `json:"formatVersion"`
	Secret        string `json:"secret"`
}

// SecretName returns the Secret name, or "" for an unencrypted volume or backup.
func (e *BackupEncryption) SecretName() string {
	if e == nil {
		return ""
	}
	return e.Secret
}

// encryptionKeyFile is encryption/key.json: the wrapped data-encryption key (DEK) and the
// metadata needed to unwrap it. Every field that affects cryptographic processing is bound
// to the wrapped DEK as additional authenticated data.
type encryptionKeyFile struct {
	FormatVersion    int    `json:"formatVersion"`
	Algorithm        string `json:"algorithm"`
	KeyProvider      string `json:"keyProvider"`
	Secret           string `json:"secret"`
	KeyFingerprint   string `json:"keyFingerprint"`
	KeyWrapAlgorithm string `json:"keyWrapAlgorithm"`
	KeyWrapNonce     string `json:"keyWrapNonce"`
	WrappedDEK       string `json:"wrappedDEK"`
}

func (f *encryptionKeyFile) additionalData() ([]byte, error) {
	return json.Marshal(struct {
		FormatVersion    int
		Algorithm        string
		KeyProvider      string
		Secret           string
		KeyFingerprint   string
		KeyWrapAlgorithm string
	}{f.FormatVersion, f.Algorithm, f.KeyProvider, f.Secret, f.KeyFingerprint, f.KeyWrapAlgorithm})
}

// keyFingerprint identifies a KEK without revealing it, so a replaced key is reported as a
// mismatch instead of surfacing as a generic authentication failure.
func keyFingerprint(kek []byte) string {
	mac := hmac.New(sha256.New, kek)
	mac.Write([]byte(keyFingerprintContext))
	return hex.EncodeToString(mac.Sum(nil))
}

func newKeyWrapAEAD(kek []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// newEncryptionKeyFile generates a random DEK and wraps it under the KEK.
func newEncryptionKeyFile(cfg *EncryptionConfig) (*encryptionKeyFile, *blockCipher, error) {
	dek := make([]byte, encryptionKeySize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, nil, errors.Wrap(err, "failed to generate backup data encryption key")
	}
	aead, err := newKeyWrapAEAD(cfg.kek)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, errors.Wrap(err, "failed to generate backup key wrap nonce")
	}

	f := &encryptionKeyFile{
		FormatVersion:    EncryptionFormatVersion,
		Algorithm:        encryptionAlgorithm,
		KeyProvider:      encryptionKeyProvider,
		Secret:           cfg.SecretName,
		KeyFingerprint:   keyFingerprint(cfg.kek),
		KeyWrapAlgorithm: encryptionKeyWrapAlgorithm,
		KeyWrapNonce:     base64.StdEncoding.EncodeToString(nonce),
	}
	aad, err := f.additionalData()
	if err != nil {
		return nil, nil, err
	}
	f.WrappedDEK = base64.StdEncoding.EncodeToString(aead.Seal(nil, nonce, dek, aad))
	return f, &blockCipher{dek: dek}, nil
}

// unwrap verifies the metadata and returns the DEK cipher. It never returns a cipher for a
// key or metadata that fails verification.
func (f *encryptionKeyFile) unwrap(cfg *EncryptionConfig) (*blockCipher, error) {
	if f.FormatVersion != EncryptionFormatVersion {
		return nil, fmt.Errorf("unsupported backup encryption format version %v", f.FormatVersion)
	}
	if f.Algorithm != encryptionAlgorithm || f.KeyWrapAlgorithm != encryptionKeyWrapAlgorithm || f.KeyProvider != encryptionKeyProvider {
		return nil, fmt.Errorf("unsupported backup encryption algorithm %q", f.Algorithm)
	}
	if cfg == nil || cfg.SecretName != f.Secret {
		return nil, fmt.Errorf("required backup encryption secret %q is unavailable", f.Secret)
	}
	if !hmac.Equal([]byte(f.KeyFingerprint), []byte(keyFingerprint(cfg.kek))) {
		return nil, fmt.Errorf("backup encryption key in secret %q does not match the key used for this backup", f.Secret)
	}

	nonce, err := base64.StdEncoding.DecodeString(f.KeyWrapNonce)
	if err != nil {
		return nil, ErrBackupDecryptionFailed
	}
	wrapped, err := base64.StdEncoding.DecodeString(f.WrappedDEK)
	if err != nil {
		return nil, ErrBackupDecryptionFailed
	}
	aead, err := newKeyWrapAEAD(cfg.kek)
	if err != nil {
		return nil, err
	}
	if len(nonce) != aead.NonceSize() {
		return nil, ErrBackupDecryptionFailed
	}
	aad, err := f.additionalData()
	if err != nil {
		return nil, err
	}
	dek, err := aead.Open(nil, nonce, wrapped, aad)
	if err != nil || len(dek) != encryptionKeySize {
		return nil, ErrBackupDecryptionFailed
	}
	return &blockCipher{dek: dek}, nil
}

// blockCipher encrypts and decrypts block payloads with the volume DEK in the DARE format.
// Each encryption uses a fresh random nonce; block reuse relies on the plaintext checksum in
// the block file name, never on ciphertext equality.
type blockCipher struct {
	dek []byte
}

func (c *blockCipher) sioConfig() sio.Config {
	return sio.Config{
		MinVersion:   sio.Version20,
		MaxVersion:   sio.Version20,
		CipherSuites: []byte{sio.AES_GCM},
		Key:          c.dek,
	}
}

func (c *blockCipher) encrypt(rs io.ReadSeeker) (io.ReadSeeker, error) {
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "failed to seek block before encryption")
	}
	var buf bytes.Buffer
	if _, err := sio.Encrypt(&buf, rs, c.sioConfig()); err != nil {
		return nil, errors.Wrap(err, "failed to encrypt block")
	}
	// DARE emits nothing for empty input, and an empty object carries no authentication tag.
	// Block payloads are never empty, so refuse rather than write an unauthenticated object.
	if buf.Len() == 0 {
		return nil, errors.New("refusing to encrypt an empty block")
	}
	return bytes.NewReader(buf.Bytes()), nil
}

func (c *blockCipher) decrypt(data []byte) ([]byte, error) {
	plaintext, err := sio.DecryptBuffer(make([]byte, 0, len(data)), data, c.sioConfig())
	if err != nil {
		return nil, ErrBackupDecryptionFailed
	}
	return plaintext, nil
}

func getEncryptionPath(volumeName string) string {
	return filepath.Join(getVolumePath(volumeName), ENCRYPTION_DIRECTORY) + "/"
}

func getEncryptionKeyFilePath(volumeName string) string {
	return filepath.Join(getEncryptionPath(volumeName), ENCRYPTION_KEY_FILE)
}

func loadVolumeCipher(driver BackupStoreDriver, volumeName string, cfg *EncryptionConfig) (*blockCipher, error) {
	f := &encryptionKeyFile{}
	if err := LoadConfigInBackupStore(driver, getEncryptionKeyFilePath(volumeName), f); err != nil {
		return nil, errors.Wrapf(err, "failed to load backup encryption metadata of volume %v", volumeName)
	}
	return f.unwrap(cfg)
}

// volumeHasBlocks reports whether any block was ever written for the volume, including
// blocks left behind by a backup that failed before it completed.
func volumeHasBlocks(driver BackupStoreDriver, volumeName string) (bool, error) {
	blockPath := getBlockPath(volumeName)
	dirs, err := driver.List(blockPath)
	if err != nil {
		// Filesystem drivers return an error for a missing directory. Treat the error as
		// "no blocks" only when the directory is really absent.
		if driver.FileExists(blockPath) {
			return false, errors.Wrapf(err, "failed to list blocks of volume %v", volumeName)
		}
		return false, nil
	}
	return len(dirs) > 0, nil
}

// reconcileBackupEncryption applies the requested encryption to a volume's established
// encryption state, and returns the block cipher to use, or nil for an unencrypted backup.
//
// The encryption state is fixed by the volume's first backup, so every block in the
// volume's blocks/ directory is uniformly encrypted or unencrypted:
//   - established encrypted: the request must use the same Secret;
//   - established unencrypted (backups or blocks already exist): the request must be unencrypted;
//   - no history: the request establishes the state, recorded before any block is written.
func reconcileBackupEncryption(driver BackupStoreDriver, volume *Volume, cfg *EncryptionConfig) (*blockCipher, error) {
	if volume.Encryption != nil {
		if volume.Encryption.FormatVersion > EncryptionFormatVersion {
			return nil, fmt.Errorf("unsupported backup encryption format version %v", volume.Encryption.FormatVersion)
		}
		if cfg == nil || cfg.SecretName != volume.Encryption.Secret {
			return nil, fmt.Errorf("backup encryption secret for volume %q cannot change from %q", volume.Name, volume.Encryption.Secret)
		}
		return loadVolumeCipher(driver, volume.Name, cfg)
	}

	hasBlocks, err := volumeHasBlocks(driver, volume.Name)
	if err != nil {
		return nil, err
	}
	if volume.LastBackupName != "" || hasBlocks {
		if cfg != nil {
			return nil, fmt.Errorf("backup encryption secret for volume %q cannot change from unencrypted", volume.Name)
		}
		return nil, nil
	}

	if cfg == nil {
		return nil, nil
	}

	// First backup of an encrypted volume: persist the wrapped DEK, then record the state in
	// volume.cfg. Both happen before any block is written, so a crash in between leaves no
	// encrypted block behind and the next attempt simply creates a new key file.
	keyFile, blkCipher, err := newEncryptionKeyFile(cfg)
	if err != nil {
		return nil, err
	}
	if err := SaveConfigInBackupStore(driver, getEncryptionKeyFilePath(volume.Name), keyFile); err != nil {
		return nil, errors.Wrapf(err, "failed to save backup encryption metadata of volume %v", volume.Name)
	}
	volume.Encryption = &BackupEncryption{FormatVersion: EncryptionFormatVersion, Secret: cfg.SecretName}
	if err := saveVolume(driver, volume); err != nil {
		return nil, errors.Wrapf(err, "failed to record backup encryption state of volume %v", volume.Name)
	}
	return blkCipher, nil
}

// loadBackupCipher returns the cipher needed to read a backup, decided only by the backup's
// own metadata. It returns nil for an unencrypted backup.
func loadBackupCipher(driver BackupStoreDriver, volumeName string, enc *BackupEncryption, cfg *EncryptionConfig) (*blockCipher, error) {
	if enc == nil {
		return nil, nil
	}
	if enc.FormatVersion > EncryptionFormatVersion {
		return nil, fmt.Errorf("unsupported backup encryption format version %v", enc.FormatVersion)
	}
	if cfg == nil || cfg.SecretName != enc.Secret {
		return nil, fmt.Errorf("required backup encryption secret %q is unavailable", enc.Secret)
	}
	return loadVolumeCipher(driver, volumeName, cfg)
}
