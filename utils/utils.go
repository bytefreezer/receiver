// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package utils

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"path/filepath"
	"strings"
)

// CompressData compresses data using gzip
func CompressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err := gz.Write(data); err != nil {
		return nil, err
	}

	if err := gz.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ValidateFilePath validates that a file path is safe and within expected directories
// This prevents directory traversal attacks (CWE-22)
func ValidateFilePath(filePath, allowedBasePath string) error {
	// Clean the paths to resolve any .. or . elements
	cleanPath := filepath.Clean(filePath)
	cleanBasePath := filepath.Clean(allowedBasePath)

	// Check for actual path traversal attempts by comparing original vs cleaned path
	// If filepath.Clean() changed the path, it might have resolved traversal attempts
	if cleanPath != filePath {
		// Only check for .. in path components, not filenames
		pathParts := strings.Split(filepath.Dir(cleanPath), string(filepath.Separator))
		for _, part := range pathParts {
			if part == ".." {
				return fmt.Errorf("path traversal detected in file path: %s", filePath)
			}
		}
	}

	// Ensure the path is within the allowed base path
	if !strings.HasPrefix(cleanPath, cleanBasePath) {
		return fmt.Errorf("file path %s is outside allowed directory %s", filePath, allowedBasePath)
	}

	// Additional security checks
	if strings.ContainsAny(filePath, "\x00\r\n") {
		return fmt.Errorf("invalid characters in file path: %s", filePath)
	}

	return nil
}

// ExtractFileExtension parses file extension from proxy filename format
func ExtractFileExtension(filename string) string {
	// Remove path if present
	basename := filepath.Base(filename)

	// Remove .gz suffix
	basename = strings.TrimSuffix(basename, ".gz")

	// Split by -- separator for new format: tenant--dataset--timestamp--extension.gz
	parts := strings.Split(basename, "--")
	if len(parts) >= 4 {
		return parts[3] // extension is 4th part
	}

	// Fallback: try old format batch_id.extension.gz
	if strings.Contains(basename, ".") {
		parts := strings.Split(basename, ".")
		if len(parts) >= 2 {
			return parts[len(parts)-1] // last part before .gz
		}
	}

	return ""
}
