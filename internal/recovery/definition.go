// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sassoftware/phorcys/internal/broker"
)

// definitionFileName is the backup-dir file Prepare writes the captured
// definition to, so Execute can recreate the queue even after a restart or
// on another node.
const definitionFileName = "definition.json"

// QueueDefinition is the durable record of what's needed to recreate a queue
// after deletion: its declare arguments/durability and the bindings pointing
// to it, captured from the broker's definitions export (metadata store)
// rather than the queue's own process, so it's available even when the queue
// is UNRECOVERABLE.
type QueueDefinition struct {
	Durable    bool                  `json:"durable"`
	AutoDelete bool                  `json:"auto_delete"`
	Arguments  map[string]any        `json:"arguments"`
	Bindings   []broker.QueueBinding `json:"bindings"`
}

// FetchDefinition exports vhost's definitions and extracts queueName's declare
// arguments and bindings. It returns nil, nil — not an error — when the queue
// isn't present in the export, so callers can fall back to a minimal declare.
func FetchDefinition(ctx context.Context, dm *broker.DiagnosticsManager, vhost, queueName string) (*QueueDefinition, error) {
	def, bindings, err := dm.FetchQueueDefinition(ctx, vhost, queueName)
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, nil
	}
	return &QueueDefinition{
		Durable:    def.Durable,
		AutoDelete: def.AutoDelete,
		Arguments:  def.Arguments,
		Bindings:   bindings,
	}, nil
}

// SaveDefinition writes qd as JSON to definition.json in backupDir. A nil qd
// is a no-op, so callers don't need to special-case a definition that could
// not be fetched.
func SaveDefinition(backupDir string, qd *QueueDefinition) error {
	if qd == nil {
		return nil
	}
	data, err := json.MarshalIndent(qd, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling definition: %w", err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, definitionFileName), data, 0600); err != nil {
		return fmt.Errorf("writing definition file: %w", err)
	}
	return nil
}

// LoadDefinition reads definition.json from backupDir. It returns nil, nil —
// not an error — when the file is absent (e.g. the definition could not be
// fetched during Prepare), so callers can fall back to a minimal declare.
func LoadDefinition(backupDir string) (*QueueDefinition, error) {
	data, err := os.ReadFile(filepath.Join(backupDir, definitionFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading definition file: %w", err)
	}

	// UseNumber preserves the int-vs-float distinction in arguments (e.g.
	// x-max-length) across the JSON round trip, matching FetchQueueDefinition.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var qd QueueDefinition
	if err := dec.Decode(&qd); err != nil {
		return nil, fmt.Errorf("unmarshaling definition file: %w", err)
	}
	return &qd, nil
}
