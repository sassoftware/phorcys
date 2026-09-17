// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"time"
)

// DeleteOrForceEvict attempts HTTP deletion first, then escalates to Erlang runtime eviction.
func (dm *DiagnosticsManager) DeleteOrForceEvict(ctx context.Context, vhost, queueName string) error {
	escapedVHost := url.PathEscape(vhost)
	escapedQueue := url.PathEscape(queueName)
	endpoint := fmt.Sprintf("/api/queues/%s/%s", escapedVHost, escapedQueue)

	log.Printf("[Delete] Standard HTTP deletion for %s/%s", vhost, queueName)
	if err := dm.deleteQueue(ctx, endpoint); err == nil {
		log.Printf("[Delete] HTTP deletion successful for %s", queueName)
		return nil
	} else {
		log.Printf("[Delete] HTTP delete failed (%v) — escalating to Erlang runtime eviction", err)
	}

	if err := dm.forceDeleteQueue(ctx, vhost, queueName); err != nil {
		return fmt.Errorf("both standard and force-deletion failed: %w", err)
	}

	log.Printf("[Delete] Force eviction completed for %s", queueName)
	return nil
}

// deleteQueue sends a DELETE request to the management API.
func (dm *DiagnosticsManager) deleteQueue(ctx context.Context, endpoint string) error {
	req, err := dm.newRequest(ctx, http.MethodDelete, endpoint)
	if err != nil {
		return err
	}

	resp, err := dm.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

// forceDeleteQueue bypasses the HTTP stack and interacts directly with the local Erlang runtime
func (dm *DiagnosticsManager) forceDeleteQueue(ctx context.Context, vhost, queueName string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Erlang script for RabbitMQ:
	// - Attempts to look up the queue in memory.
	// - If found, calls delete_crashed to cleanly shut down Raft log records and files.
	// - If the process has vanished but left orphaned Mnesia records, it performs a dirty metadata purge.
	erlangExpression := fmt.Sprintf(`
		Resource = rabbit_misc:r(<<"%s">>, queue, <<"%s">>),
		case rabbit_amqqueue:lookup(Resource) of
			{ok, Q} -> 
				rabbit_amqqueue:delete_crashed(Q);
			_ -> 
				mnesia:dirty_delete(rabbit_durable_queue, Resource),
				mnesia:dirty_delete(rabbit_queue, Resource)
		end.
	`, vhost, queueName)

	cmd := exec.CommandContext(cmdCtx, "rabbitmqctl", "eval", erlangExpression)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("rabbitmqctl eval execution error: %v | Output: %s", err, string(output))
	}

	return nil
}
