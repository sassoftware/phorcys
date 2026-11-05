// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func clearTLSEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{EnvVarTLSCAFile, EnvVarTLSCertFile, EnvVarTLSKeyFile, EnvVarTLSServerName, EnvVarTLSSkipVerify} {
		t.Setenv(k, "")
	}
}

// writeSelfSigned writes a self-signed cert and key to dir and returns their paths.
func writeSelfSigned(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath
}

func TestLoadTLSConfig_UnsetReturnsNil(t *testing.T) {
	clearTLSEnv(t)

	cfg, err := loadTLSConfig()

	require.NoError(t, err)
	assert.Nil(t, cfg)
}

func TestLoadTLSConfig_CAAndClientCert(t *testing.T) {
	clearTLSEnv(t)
	certPath, keyPath := writeSelfSigned(t, t.TempDir())
	t.Setenv(EnvVarTLSCAFile, certPath)
	t.Setenv(EnvVarTLSCertFile, certPath)
	t.Setenv(EnvVarTLSKeyFile, keyPath)
	t.Setenv(EnvVarTLSServerName, "rabbit.example")

	cfg, err := loadTLSConfig()

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.RootCAs)
	assert.Len(t, cfg.Certificates, 1)
	assert.Equal(t, "rabbit.example", cfg.ServerName)
	assert.False(t, cfg.InsecureSkipVerify)
}

func TestLoadTLSConfig_SkipVerify(t *testing.T) {
	clearTLSEnv(t)
	t.Setenv(EnvVarTLSSkipVerify, "true")

	cfg, err := loadTLSConfig()

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.True(t, cfg.InsecureSkipVerify)
}

func TestLoadTLSConfig_Errors(t *testing.T) {
	certPath, _ := writeSelfSigned(t, t.TempDir())
	notPEM := filepath.Join(t.TempDir(), "bad.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("nope"), 0o600))

	tests := []struct {
		name string
		env  map[string]string
	}{
		{"missing CA file", map[string]string{EnvVarTLSCAFile: "/does/not/exist.pem"}},
		{"CA file without certs", map[string]string{EnvVarTLSCAFile: notPEM}},
		{"cert without key", map[string]string{EnvVarTLSCertFile: certPath}},
		{"bad skip-verify value", map[string]string{EnvVarTLSSkipVerify: "maybe"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearTLSEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg, err := loadTLSConfig()

			assert.Error(t, err)
			assert.Nil(t, cfg)
		})
	}
}
