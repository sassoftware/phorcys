package runtime

import "testing"

// ---------------------------------------------------------------------------
// GetEnv
// ---------------------------------------------------------------------------

func TestGetEnv_ReturnsFallbackWhenUnset(t *testing.T) {
	got := GetEnv("__ARGUS_NONEXISTENT_VAR_XYZ__", "fallback")
	if got != "fallback" {
		t.Errorf("got %q, want %q", got, "fallback")
	}
}

func TestGetEnv_ReturnsEnvValueWhenSet(t *testing.T) {
	t.Setenv("__ARGUS_TEST_VAR__", "custom_value")
	got := GetEnv("__ARGUS_TEST_VAR__", "fallback")
	if got != "custom_value" {
		t.Errorf("got %q, want %q", got, "custom_value")
	}
}

func TestGetEnv_EmptyEnvValueUsesFallback(t *testing.T) {
	// When the env var is set to empty string, the fallback should be used.
	t.Setenv("__ARGUS_EMPTY_VAR__", "")
	got := GetEnv("__ARGUS_EMPTY_VAR__", "fallback")
	if got != "fallback" {
		t.Errorf("expected fallback for empty env var, got %q", got)
	}
}
