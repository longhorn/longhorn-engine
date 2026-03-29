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
)

const LuksTimeout = time.Minute
