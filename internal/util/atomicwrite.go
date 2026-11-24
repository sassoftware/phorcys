// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"os"
	"path/filepath"
)

// WriteFileAtomic durably replaces the file at path with data. It writes to
// path+".tmp" in the same directory, fsyncs it, renames it over path, and
// fsyncs the directory, so after a crash a reader sees either the previous
// complete contents or the new complete contents, never a partial file. A
// stale temp file left by an earlier crash is overwritten.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if err = os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return SyncDir(filepath.Dir(path))
}

// SyncDir fsyncs the directory at dir so that entries created, renamed or
// removed in it are durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if closeErr := d.Close(); err == nil {
		err = closeErr
	}
	return err
}
