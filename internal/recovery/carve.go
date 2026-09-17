package recovery

import (
	"log"
	"os"
	"path/filepath"
)

// CarveMessagesFromDir scans a directory for .segment and .wal files and extracts
// all recoverable AMQP message payloads from each using ETF carving.
func CarveMessagesFromDir(dirPath string) ([][]byte, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var allPayloads [][]byte
	for _, f := range files {
		ext := filepath.Ext(f.Name())
		if ext != ".segment" && ext != ".wal" {
			continue
		}
		payloads, err := CarveMessagesFromFile(filepath.Join(dirPath, f.Name()))
		if err != nil {
			log.Printf("[Carve] WARNING: Skipping %s: %v", f.Name(), err)
			continue
		}
		allPayloads = append(allPayloads, payloads...)
	}
	return allPayloads, nil
}

// CarveMessagesFromFile scans a single binary file for {content,6,...,[<<payload>>]} tuples.
//
// RabbitMQ 4.x wraps every message in Erlang map terms that the ETF library cannot decode.
func CarveMessagesFromFile(filePath string) (payloads [][]byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Carve] WARNING: recovered from panic in %s: %v", filePath, r)
			err = nil
		}
	}()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	return scanContentTuples(data), nil
}
