// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"net/url"
)

// definitionsExport mirrors the subset of the /api/definitions/{vhost} response
// body phorcys needs.
type definitionsExport struct {
	Queues   []QueueDefinition `json:"queues"`
	Bindings []QueueBinding    `json:"bindings"`
}

// FetchQueueDefinition exports vhost's definitions from the broker's metadata
// store (Mnesia/Khepri) rather than contacting the queue's own process, so it
// still succeeds when the queue itself is about to be classified UNRECOVERABLE.
// It returns a nil definition and no error when queueName is not present in the
// export (e.g. it was already deleted).
func (dm *DiagnosticsManager) FetchQueueDefinition(ctx context.Context, vhost, queueName string) (*QueueDefinition, []QueueBinding, error) {
	endpoint := fmt.Sprintf("/api/definitions/%s", url.PathEscape(vhost))
	// UseNumber preserves the int-vs-float distinction in queue/binding arguments
	// (e.g. x-max-length) so callers don't re-declare them as the wrong AMQP type.
	export, err := fetchJSON[definitionsExport](ctx, dm, endpoint, true)
	if err != nil {
		return nil, nil, err
	}

	var def *QueueDefinition
	for i := range export.Queues {
		if export.Queues[i].Name == queueName {
			def = &export.Queues[i]
			break
		}
	}
	if def == nil {
		return nil, nil, nil
	}

	var bindings []QueueBinding
	for _, b := range export.Bindings {
		if b.DestinationType == "queue" && b.Destination == queueName {
			bindings = append(bindings, b)
		}
	}

	return def, bindings, nil
}
