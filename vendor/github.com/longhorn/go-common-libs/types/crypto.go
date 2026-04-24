package types

import (
	"time"
)

const (
	CryptoKeyProvider          = "CRYPTO_KEY_PROVIDER"
	CryptoKeyValue             = "CRYPTO_KEY_VALUE"
	CryptoKeyCipher            = "CRYPTO_KEY_CIPHER"
	CryptoKeyHash              = "CRYPTO_KEY_HASH"
	CryptoKeySize              = "CRYPTO_KEY_SIZE"
	CryptoPBKDF                = "CRYPTO_PBKDF"
	CryptoPBKDFForceIterations = "CRYPTO_PBKDF_FORCE_ITERATIONS"
	CryptoPBKDFMemory          = "CRYPTO_PBKDF_MEMORY"

	CliAPIVersionForSupportingExtendLuks2HeaderSize = 12
	Luks2EncryptionHeaderSize                       = 16 * 1024 * 1024
)

const LuksTimeout = time.Minute

func GetBackendSize(volumeSize int64, encrypted bool, cliAPIVersion int) int64 {
	if volumeSize > 0 && encrypted && cliAPIVersion >= CliAPIVersionForSupportingExtendLuks2HeaderSize {
		//  The default size is 16MB for the LUKS2 header, so we need to add it to the replica size if the volume is encrypted.
		//  otherwise, the device that users get will be 16MB smaller than the actual size users want, which will cause some issues as https://github.com/longhorn/longhorn/issues/9205.
		//  https://gitlab.com/cryptsetup/cryptsetup/-/wikis/FrequentlyAskedQuestions
		return volumeSize + Luks2EncryptionHeaderSize
	}
	return volumeSize
}
