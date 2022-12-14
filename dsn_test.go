package godatabend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatDSN(t *testing.T) {
	dsn := "databend+https://username:password@tn3ftqihs.ch.aws-us-east-2.default.databend.com/test?warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings"
	cfg, err := ParseDSN(dsn)
	require.Nil(t, err)

	assert.Equal(t, "wh", cfg.Warehouse)
	assert.Equal(t, "tn3ftqihs.ch.aws-us-east-2.default.databend.com:443", cfg.Host)
	assert.Equal(t, "test", cfg.Database)
	assert.Equal(t, "tls-settings", cfg.TLSConfig)
	assert.Equal(t, time.Second, cfg.Timeout)
	assert.Equal(t, time.Second*2, cfg.IdleTimeout)

	dsn1 := cfg.FormatDSN()
	assert.Equal(t, "databend://username:password@tn3ftqihs.ch.aws-us-east-2.default.databend.com:443/test?idle_timeout=2s&timeout=1s&tls_config=tls-settings&warehouse=wh", dsn1)

	cfg1, err := ParseDSN(dsn1)
	require.Nil(t, err)
	assert.Equal(t, cfg, cfg1)
}

func TestConfigURL(t *testing.T) {
	dsn := "databend+https://username:password@app.databend.com:443/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings"
	cfg, err := ParseDSN(dsn)
	require.Nil(t, err)

	u1 := cfg.url(map[string]string{"default_format": "Native"}).String()
	assert.Equal(t, "https://username:password@app.databend.com:443/?database=test&default_format=Native", u1)
}

func TestParseDSN(t *testing.T) {
	dsn := "databend://username:password@app.databend.com:8000/test?idle_timeout=2s&sslmode=disable&tenant=tn&timeout=1s&tls_config=tls-settings&warehouse=wh"

	tests := []string{
		"databend+http://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings",
		"db+http://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings",
		"bend+http://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings",
		"http://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings",
		"databend://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings&sslmode=disable",
		"db://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings&sslmode=disable",
		"dd://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings&sslmode=disable",
		"bend://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings&sslmode=disable",
		"https://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&idle_timeout=2s&tls_config=tls-settings&sslmode=disable",
	}

	for _, test := range tests {
		cfg, err := ParseDSN(test)
		require.Nil(t, err)

		assert.Equal(t, "username", cfg.User)
		assert.Equal(t, "password", cfg.Password)
		assert.Equal(t, "tn", cfg.Tenant)
		assert.Equal(t, "wh", cfg.Warehouse)
		assert.Equal(t, "app.databend.com:8000", cfg.Host)
		assert.Equal(t, "test", cfg.Database)
		assert.Equal(t, "tls-settings", cfg.TLSConfig)
		assert.Equal(t, SSL_MODE_DISABLE, cfg.SSLMode)
		assert.Equal(t, time.Second, cfg.Timeout)

		assert.Equal(t, dsn, cfg.FormatDSN())
	}
}
