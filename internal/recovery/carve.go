// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"log"
	"os"
	"path/filepath"

	amqp "github.com/rabbitmq/amqp091-go"
)

// CarveMessagesFromDir scans a directory for .segment and .wal files and extracts
// all recoverable AMQP messages — payload plus original properties (including
// headers) — from each using ETF carving.
func CarveMessagesFromDir(dirPath string) ([]amqp.Publishing, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var allMessages []amqp.Publishing
	for _, f := range files {
		ext := filepath.Ext(f.Name())
		if ext != segmentFileSuffix && ext != walFileSuffix {
			continue
		}
		messages, err := CarveMessagesFromFile(filepath.Join(dirPath, f.Name()))
		if err != nil {
			log.Printf("[Carve] WARNING: Skipping %s: %v", f.Name(), err)
			continue
		}
		allMessages = append(allMessages, messages...)
	}
	return allMessages, nil
}

// CarveMessagesFromFile scans a single binary file for {content,6,...,[<<payload>>]} tuples
// and returns each recovered message with its original properties (including headers).
//
// RabbitMQ 4.x wraps every message in Erlang map terms that the ETF library cannot decode.
func CarveMessagesFromFile(filePath string) (messages []amqp.Publishing, err error) {
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

	return scanContentMessages(data), nil
}
