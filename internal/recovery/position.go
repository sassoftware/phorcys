// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// RaftPosition identifies a point in a Ra server's replicated log.
type RaftPosition struct {
	Idx  uint64
	Term uint64
}

// After reports whether p is more up-to-date than other, using the same
// precedence Raft uses to pick the newest log: the higher term always wins,
// and the higher index only decides a tie on term.
func (p RaftPosition) After(other RaftPosition) bool {
	if p.Term != other.Term {
		return p.Term > other.Term
	}
	return p.Idx > other.Idx
}

// LatestQueuePosition returns the highest Raft log position (by term, then
// index) found across a queue's backed-up .segment files (segmentDir) and,
// filtered to queueUID, its backed-up .wal files (walDir). Either directory
// may be passed as "" to skip that source (e.g. when no WAL backup exists).
//
// This is a standalone, read-only query over existing backup files: it never
// touches the broker and is independent of CarveMessagesFromDir/CarveWALMessages/
// RepublishMessages, so it is safe to call on its own — e.g. by cluster
// comparison tooling deciding which node holds the most recent data for a
// queue — without affecting the carve/republish pipeline.
//
// A file that fails to parse (e.g. corrupted/truncated) is logged and
// skipped rather than aborting the whole scan, since backups can legitimately
// contain a mix of healthy and damaged files.
//
// found is false if no segment or WAL records were present for this queue.
func LatestQueuePosition(segmentDir, walDir, queueUID string) (latest RaftPosition, found bool, err error) {
	segPos, segFound, err := latestSegmentPosition(segmentDir)
	if err != nil {
		return RaftPosition{}, false, err
	}
	walPos, walFound, err := latestWALPosition(walDir, queueUID)
	if err != nil {
		return RaftPosition{}, false, err
	}

	switch {
	case segFound && walFound:
		if walPos.After(segPos) {
			return walPos, true, nil
		}
		return segPos, true, nil
	case segFound:
		return segPos, true, nil
	case walFound:
		return walPos, true, nil
	default:
		return RaftPosition{}, false, nil
	}
}

// latestSegmentPosition scans every .segment file in dir and returns the
// highest (term, index) position across all of them.
func latestSegmentPosition(dir string) (RaftPosition, bool, error) {
	if dir == "" {
		return RaftPosition{}, false, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return RaftPosition{}, false, nil
		}
		return RaftPosition{}, false, fmt.Errorf("reading segment dir: %w", err)
	}

	var latest RaftPosition
	found := false
	total, failed := 0, 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != segmentFileSuffix {
			continue
		}
		total++
		records, err := ParseSegmentRecords(filepath.Join(dir, entry.Name()))
		if err != nil {
			failed++
			log.Printf("[Position] WARNING: skipping unparsable segment file %s: %v", entry.Name(), err)
			continue
		}
		for _, r := range records {
			pos := RaftPosition{Idx: r.Idx, Term: r.Term}
			if !found || pos.After(latest) {
				latest = pos
				found = true
			}
		}
	}
	if total > 0 && failed == total {
		log.Printf("[Position] WARNING: all %d segment file(s) in %s failed to parse — Raft position unknown, not just absent", total, dir)
	}
	return latest, found, nil
}

// latestWALPosition scans every .wal file in dir for records belonging to
// queueUID and returns the highest (term, index) position among them.
func latestWALPosition(dir, queueUID string) (RaftPosition, bool, error) {
	if dir == "" {
		return RaftPosition{}, false, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return RaftPosition{}, false, nil
		}
		return RaftPosition{}, false, fmt.Errorf("reading WAL dir: %w", err)
	}

	var latest RaftPosition
	found := false
	total, failed := 0, 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != walFileSuffix {
			continue
		}
		total++
		records, err := ParseWALRecords(filepath.Join(dir, entry.Name()), queueUID)
		if err != nil {
			failed++
			log.Printf("[Position] WARNING: skipping unparsable WAL file %s: %v", entry.Name(), err)
			continue
		}
		for _, r := range records {
			pos := RaftPosition{Idx: r.Idx, Term: r.Term}
			if !found || pos.After(latest) {
				latest = pos
				found = true
			}
		}
	}
	if total > 0 && failed == total {
		log.Printf("[Position] WARNING: all %d WAL file(s) in %s failed to parse — Raft position unknown, not just absent", total, dir)
	}
	return latest, found, nil
}
