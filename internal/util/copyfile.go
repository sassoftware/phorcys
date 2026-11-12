// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// availableSpace reports the bytes available to an unprivileged user on the
// filesystem containing path. ok is false when the platform cannot report it.
// It is a variable so tests can substitute it.
var availableSpace = diskAvailable

// CopyFile copies the contents of src to dst, creating or truncating dst as needed.
// Before writing, it verifies the destination filesystem has enough free space.
func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	srcInfo, err := in.Stat()
	if err != nil {
		return err
	}

	if err = ensureSpace(dst, srcInfo.Size()); err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return nil
}

// ensureSpace returns an error if the filesystem holding dst cannot accommodate
// the needed number of bytes. Space reclaimed by truncating an existing regular
// file at dst is credited toward the requirement.
func ensureSpace(dst string, need int64) error {
	// Does dst exist and is it a regular file? If so, we can reclaim its space.
	if info, err := os.Stat(dst); err == nil && info.Mode().IsRegular() {
		need -= info.Size()
	}
	if need <= 0 {
		return nil
	}

	// Check available space on the filesystem containing dst.
	avail, err := availableSpace(filepath.Dir(dst))
	if err != nil {
		return fmt.Errorf("checking free space for %s: %w", dst, err)
	}
	// Is there enough space to accommodate the copy?
	if uint64(need) > avail {
		return fmt.Errorf("insufficient disk space: copying to %s needs %d bytes, %d available",
			dst, need, avail)
	}
	return nil
}
