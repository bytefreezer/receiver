package utils

import (
	"strings"
	"testing"
)

func TestExtractFileExtension(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
		expected string
	}{
		{
			name:     "new format - raw",
			filename: "acme--logs--1736938245123456789--raw.gz",
			expected: "raw",
		},
		{
			name:     "new format - csv",
			filename: "company--metrics--1736938245123456789--csv.gz",
			expected: "csv",
		},
		{
			name:     "new format - ndjson",
			filename: "tenant1--dataset1--1736938245123456789--ndjson.gz",
			expected: "ndjson",
		},
		{
			name:     "new format - json",
			filename: "acme--api--1736938245123456789--json.gz",
			expected: "json",
		},
		{
			name:     "old format - raw",
			filename: "batch_123456789.raw.gz",
			expected: "raw",
		},
		{
			name:     "old format - csv",
			filename: "batch_987654321.csv.gz",
			expected: "csv",
		},
		{
			name:     "old format - ndjson",
			filename: "batch_111222333.ndjson.gz",
			expected: "ndjson",
		},
		{
			name:     "malformed - missing extension (double dots)",
			filename: "batch_123456789..gz",
			expected: "",
		},
		{
			name:     "malformed - no extension part",
			filename: "batch_123456789.gz",
			expected: "",
		},
		{
			name:     "malformed - no structure",
			filename: "invalid-filename.gz",
			expected: "",
		},
		{
			name:     "with full path",
			filename: "/var/spool/bytefreezer-receiver/acme/logs/queue/acme--logs--123456789--json.gz",
			expected: "json",
		},
		{
			name:     "no .gz suffix",
			filename: "acme--logs--123456789--xml",
			expected: "xml",
		},
		{
			name:     "empty filename",
			filename: "",
			expected: "",
		},
		{
			name:     "only extension - fallback to old format",
			filename: "raw.gz",
			expected: "", // This should not match any format
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ExtractFileExtension(tc.filename)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s for filename %s", tc.expected, result, tc.filename)
			}
		})
	}
}

func TestFilenameValidation(t *testing.T) {
	// Test filename validation logic that would be used in webhook handlers
	testCases := []struct {
		name        string
		filename    string
		urlTenant   string
		urlDataset  string
		shouldMatch bool
	}{
		{
			name:        "valid match",
			filename:    "acme--logs--123456789--raw.gz",
			urlTenant:   "acme",
			urlDataset:  "logs",
			shouldMatch: true,
		},
		{
			name:        "tenant mismatch",
			filename:    "company--logs--123456789--raw.gz",
			urlTenant:   "acme",
			urlDataset:  "logs",
			shouldMatch: false,
		},
		{
			name:        "dataset mismatch",
			filename:    "acme--metrics--123456789--raw.gz",
			urlTenant:   "acme",
			urlDataset:  "logs",
			shouldMatch: false,
		},
		{
			name:        "old format - no validation possible",
			filename:    "batch_123456789.raw.gz",
			urlTenant:   "acme",
			urlDataset:  "logs",
			shouldMatch: false, // Can't validate old format
		},
		{
			name:        "malformed - missing parts",
			filename:    "acme--logs.gz",
			urlTenant:   "acme",
			urlDataset:  "logs",
			shouldMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate the validation logic from webhook handlers
			isValid := false
			if len(tc.filename) > 0 && tc.filename[0] != '.' {
				parts := make([]string, 0)
				if len(tc.filename) > 3 && tc.filename[len(tc.filename)-3:] == ".gz" {
					base := tc.filename[:len(tc.filename)-3]
					parts = strings.Split(base, "--")
				}

				if len(parts) >= 4 && parts[0] == tc.urlTenant && parts[1] == tc.urlDataset {
					isValid = true
				}
			}

			if isValid != tc.shouldMatch {
				t.Errorf("Expected validation result %t, got %t for filename %s", tc.shouldMatch, isValid, tc.filename)
			}
		})
	}
}

func TestMalformedFilenameDetection(t *testing.T) {
	// Test the malformed filename detection logic
	testCases := []struct {
		name        string
		filename    string
		isMalformed bool
	}{
		{
			name:        "valid new format",
			filename:    "acme--logs--123456789--raw.gz",
			isMalformed: false,
		},
		{
			name:        "valid old format",
			filename:    "batch_123456789.raw.gz",
			isMalformed: false,
		},
		{
			name:        "malformed - double dots",
			filename:    "acme--logs--123456789..gz",
			isMalformed: true,
		},
		{
			name:        "malformed - old format double dots",
			filename:    "batch_123456789..gz",
			isMalformed: true,
		},
		{
			name:        "valid with dots in extension",
			filename:    "acme--logs--123456789--file.json.gz",
			isMalformed: false,
		},
		{
			name:        "malformed - consecutive dots",
			filename:    "file..with..dots.gz",
			isMalformed: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Check for malformed filenames (contains "..")
			containsDoubleDots := strings.Contains(tc.filename, "..")

			if containsDoubleDots != tc.isMalformed {
				t.Errorf("Expected malformed detection %t, got %t for filename %s", tc.isMalformed, containsDoubleDots, tc.filename)
			}
		})
	}
}
