// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
)

const (
	EnvVarTLSCAFile     = "PHORCYS_TLS_CA_FILE"
	EnvVarTLSCertFile   = "PHORCYS_TLS_CERT_FILE"
	EnvVarTLSKeyFile    = "PHORCYS_TLS_KEY_FILE"
	EnvVarTLSServerName = "PHORCYS_TLS_SERVER_NAME"
	EnvVarTLSSkipVerify = "PHORCYS_TLS_INSECURE_SKIP_VERIFY"
)

// loadTLSConfig builds the client TLS settings shared by the AMQP connection
// and the Management API client. It returns nil when no PHORCYS_TLS_* variable
// is set, in which case amqps:// and https:// URLs use Go's defaults (system
// trust store). The result is ignored for plain amqp:// and http:// URLs.
func loadTLSConfig() (*tls.Config, error) {
	caFile := os.Getenv(EnvVarTLSCAFile)
	certFile := os.Getenv(EnvVarTLSCertFile)
	keyFile := os.Getenv(EnvVarTLSKeyFile)
	serverName := os.Getenv(EnvVarTLSServerName)
	skipVerifyRaw := os.Getenv(EnvVarTLSSkipVerify)

	if caFile == "" && certFile == "" && keyFile == "" && serverName == "" && skipVerifyRaw == "" {
		return nil, nil
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}

	if skipVerifyRaw != "" {
		skip, err := strconv.ParseBool(skipVerifyRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid %s=%q: %w", EnvVarTLSSkipVerify, skipVerifyRaw, err)
		}
		cfg.InsecureSkipVerify = skip
	}

	if caFile != "" {
		pem, err := os.ReadFile(caFile) //nolint:gosec // operator-supplied path
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", EnvVarTLSCAFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s=%s contains no valid PEM certificates", EnvVarTLSCAFile, caFile)
		}
		cfg.RootCAs = pool
	}

	if (certFile == "") != (keyFile == "") {
		return nil, errors.New(EnvVarTLSCertFile + " and " + EnvVarTLSKeyFile + " must be set together")
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
