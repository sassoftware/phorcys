package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/DeedleFake/etf"
)

// resourcePattern matches the Erlang resource tuple in a ra 'config' file:
//
//	{resource,<<"VHOST">>,queue,<<"QUEUENAME">>}
var resourcePattern = regexp.MustCompile(`\{resource,<<"([^"]*)">>,[^,]*,<<"([^"]*)">>}`)

// QueueLocation describes a quorum queue found on disk.
type QueueLocation struct {
	VHost string
	Name  string
	Path  string
}

// FindQueueDirectory returns the absolute disk path for the given vhost/queue pair.
func FindQueueDirectory(quorumBasePath, targetVHost, targetQueue string) (string, error) {
	locations, err := ScanAllQuorumDirectories(quorumBasePath)
	if err != nil {
		return "", err
	}
	for _, loc := range locations {
		if loc.VHost == targetVHost && loc.Name == targetQueue {
			return loc.Path, nil
		}
	}
	return "", fmt.Errorf("directory not found for vhost: %s, queue: %s", targetVHost, targetQueue)
}

// ScanAllQuorumDirectories reads every subfolder in quorumBasePath and extracts queue identity
// from the plain-text Erlang 'config' file written by the ra library inside each directory.
func ScanAllQuorumDirectories(quorumBasePath string) ([]QueueLocation, error) {
	entries, err := os.ReadDir(quorumBasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read quorum base path: %w", err)
	}

	var locations []QueueLocation

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		configPath := filepath.Join(quorumBasePath, entry.Name(), "config")
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			// Skip directories that are initializing or lack a config file.
			continue
		}

		if vhost, name, found := extractQueueMetadata(configBytes); found {
			locations = append(locations, QueueLocation{
				VHost: vhost,
				Name:  name,
				Path:  filepath.Join(quorumBasePath, entry.Name()),
			})
		}
	}

	return locations, nil
}

// extractQueueMetadata parses the ra 'config' text file and returns the vhost and queue name
// by matching the {resource,<<"vhost">>,queue,<<"name">>} pattern.
func extractQueueMetadata(config []byte) (string, string, bool) {
	m := resourcePattern.FindSubmatch(config)
	if m == nil {
		return "", "", false
	}
	return string(m[1]), string(m[2]), true
}

// convertToString normalises the three ways RabbitMQ can encode a string in ETF.
// Retained for use by unit tests.
func convertToString(val any) (string, bool) {
	switch v := val.(type) {
	case []byte:
		return string(v), true
	case string:
		return v, true
	case etf.Atom:
		return string(v), true
	default:
		return "", false
	}
}
