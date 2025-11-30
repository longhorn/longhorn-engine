package utils

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"golang.org/x/exp/constraints"

	"github.com/longhorn/go-common-libs/types"
)

// Contains checks if the given slice contains the given value.
func Contains[T comparable](slice []T, value T) bool {
	for _, s := range slice {
		if s == value {
			return true
		}
	}
	return false
}

// GetFunctionName returns the <package>.<function name> of the given function.
func GetFunctionName(f interface{}) string {
	value := reflect.ValueOf(f)
	if value.Kind() != reflect.Func {
		return ""
	}
	return filepath.Base(runtime.FuncForPC(value.Pointer()).Name())
}

// GetFunctionPath returns the full path of the given function.
func GetFunctionPath(f interface{}) string {
	getFn := func() string {
		return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	}
	return GetFunctionInfo(f, getFn)
}

// GetFunctionInfo take a function as interface{} and a function for getting the info.
func GetFunctionInfo(f interface{}, getInfoFn func() string) string {
	value := reflect.ValueOf(f)
	if value.Kind() != reflect.Func {
		return ""
	}
	return getInfoFn()
}

// IsStringInSlice checks if the given string 'item' is present in the 'list' of strings.
func IsStringInSlice(list []string, item string) bool {
	for _, str := range list {
		if str == item {
			return true
		}
	}
	return false
}

// RandomID returns a random string with the specified length.
// If the specified length is less than or equal to 0, the default length will
// be used.
func RandomID(randomIDLenth int) string {
	if randomIDLenth <= 0 {
		randomIDLenth = types.RandomIDDefaultLength
	}

	uuid := strings.ReplaceAll(UUID(), "-", "")

	if len(uuid) > randomIDLenth {
		uuid = uuid[:randomIDLenth]
	}
	return uuid
}

// UUID returns a random UUID string.
func UUID() string {
	return uuid.New().String()
}

// GenerateRandomNumber generates a random positive number between lower and upper.
// The return value should be between [lower, upper), and error is nil when success.
// If the error is not nil, the return value is 0.
func GenerateRandomNumber(lower, upper int64) (int64, error) {
	if lower > upper {
		return 0, errors.Errorf("invalid boundary: [%v, %v)", lower, upper)
	}

	if lower == upper {
		return lower, nil
	}

	randNum, err := rand.Int(rand.Reader, big.NewInt(upper-lower))
	if err != nil {
		return 0, err
	}
	return (lower + randNum.Int64()), nil
}

// ConvertTypeToString converts the given value to string.
func ConvertTypeToString[T any](value T) string {
	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	default:
		return fmt.Sprintf("Unsupported type: %v", v.Kind())
	}
}

// SortKeys sorts the keys of a map in ascending order.
func SortKeys[K constraints.Ordered, V any](mapObj map[K]V) ([]K, error) {
	if mapObj == nil {
		return nil, fmt.Errorf("input object cannot be nil")
	}

	keys := make([]K, 0, len(mapObj))
	for key := range mapObj {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	return keys, nil
}

// GetNumberFromMapSupportedTypes defines the numeric types supported by
// GetNumberFromMap.
//
// Only these types can be used as the generic type parameter T in
// GetNumberFromMap. The tilde (~) allows both the base types and any
// type aliases based on them.
type GetNumberFromMapSupportedTypes interface {
	~uint8 | ~uint16 | ~uint64
}

// GetNumberFromMap retrieves a numeric value of type T from a map[string]any.
//
// This function looks up the value associated with the given key in mapObj
// and returns it as type T. The generic type T must satisfy
// GetNumberFromMapSupportedTypes (i.e., the numeric types this helper supports).
//
// Behavior:
//   - If the key does not exist or the value is nil, the zero value of T is returned.
//   - If the value already has type T, it is returned directly.
//   - If the value is a float64 (common when decoding JSON), it is converted
//     to T using a direct type conversion (T(v)) and returned. Fractional parts
//     are truncated when converting to integer types.
//   - If the value is a float64 but beyond the range of T, the zero value of T
//     is returned.
//   - Any other value type results in the zero value of T being returned.
//
// Notes:
//   - This helper is useful for reading numeric values from loosely-typed maps
//     such as JSON-decoded maps, where numbers are often represented as float64.
//   - Converting from float64 to integer types may cause truncation or precision loss.
func GetNumberFromMap[T GetNumberFromMapSupportedTypes](mapObj map[string]any, key string) T {
	var zero T
	value, ok := mapObj[key]
	if !ok || value == nil {
		return zero
	}

	if out, ok := value.(T); ok {
		return out
	}

	switch v := value.(type) {
	case float64:
		// Clamp to zero if negative or beyond T’s max range.
		if v < 0 || v > float64(^zero) {
			return zero
		}
		return T(v)
	}

	return zero
}

// GetStringFromMap retrieves a string value from a map[string]any.
//
// It looks up the value associated with the given key in mapObj and returns it
// as a string. Behavior:
//   - If the key does not exist or the value is nil, it returns an empty string.
//   - If the value is already a string, it is returned directly.
//   - If the value implements fmt.Stringer, its String() method is called and returned.
//   - For any other value type, fmt.Sprint is used to convert it to a string.
//
// This function is useful for safely extracting string representations from
// loosely-typed maps, such as JSON-decoded maps.
func GetStringFromMap(mapObj map[string]any, key string) string {
	value, ok := mapObj[key]
	if !ok || value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprint(value)
	}
}
