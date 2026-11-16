// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !(linux || darwin || freebsd || netbsd || openbsd)

package util

// diskAvailable is unsupported on this platform; the space check is skipped.
func diskAvailable(string) (uint64, error) {
	return 0, nil
}
