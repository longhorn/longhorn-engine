package util

import "testing"

func TestNormalizeLonghornDataPath(t *testing.T) {
	testCases := map[string]struct {
		input    string
		expected string
	}{
		"default when empty": {
			input:    "",
			expected: DefaultLonghornDataPath,
		},
		"clean absolute path": {
			input:    "/data/longhorn/",
			expected: "/data/longhorn",
		},
		"clean nested segments": {
			input:    "/data/../data2/longhorn",
			expected: "/data2/longhorn",
		},
		"reject relative path": {
			input:    "data/longhorn",
			expected: DefaultLonghornDataPath,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if actual := normalizeLonghornDataPath(tc.input); actual != tc.expected {
				t.Fatalf("got %q, want %q", actual, tc.expected)
			}
		})
	}
}
