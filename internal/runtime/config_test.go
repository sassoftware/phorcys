// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// GetEnv
// ---------------------------------------------------------------------------

func TestGetEnv_ReturnsFallbackWhenUnset(t *testing.T) {
	got := GetEnv("__PHORCYS_NONEXISTENT_VAR_XYZ__", "fallback")
	if got != "fallback" {
		assert.Equal(t, "fallback", got, "got %q, want %q", got, "fallback")
	}
}

func TestGetEnv_ReturnsEnvValueWhenSet(t *testing.T) {
	t.Setenv("__PHORCYS_TEST_VAR__", "custom_value")
	got := GetEnv("__PHORCYS_TEST_VAR__", "fallback")
	if got != "custom_value" {
		assert.Equal(t, "custom_value", got, "got %q, want %q", got, "custom_value")
	}
}

func TestGetEnv_EmptyEnvValueUsesFallback(t *testing.T) {
	// When the env var is set to empty string, the fallback should be used.
	t.Setenv("__PHORCYS_EMPTY_VAR__", "")
	got := GetEnv("__PHORCYS_EMPTY_VAR__", "fallback")
	if got != "fallback" {
		assert.Equal(t, "fallback", got, "expected fallback for empty env var, got %q", got)
	}
}
