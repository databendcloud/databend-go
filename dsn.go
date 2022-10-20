package godatabend

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"
)

const (
	defaultClientTimeout  = 900 * time.Second // Timeout for network round trip + read out http response
	defaultLoginTimeout   = 60 * time.Second  // Timeout for retry for login EXCLUDING clientTimeout
	defaultRequestTimeout = 0 * time.Second   // Timeout for retry for request EXCLUDING clientTimeout
	defaultDomain         = "app.databend.com"
)
const (
	clientType = "Go"
)

// Config is a set of configuration parameters
type Config struct {
	Org             string // Org name
	User            string // Username
	Password        string // Password (requires User)
	Database        string // Database name
	Warehouse       string // Warehouse
	AccessToken     string `json:"accessToken"`
	RefreshToken    string `json:"refreshToken"`
	Scheme          string
	Host            string
	Timeout         time.Duration
	IdleTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	Location        *time.Location
	Debug           bool
	UseDBLocation   bool
	GzipCompression bool
	Params          map[string]string
	TLSConfig       string
}

// NewConfig creates a new config with default values
func NewConfig() *Config {
	return &Config{
		Scheme:      "https",
		Host:        fmt.Sprintf("%s:443", defaultDomain),
		IdleTimeout: time.Hour,
		Location:    time.UTC,
		Params:      make(map[string]string),
	}
}

// FormatDSN formats the given Config into a DSN string which can be passed to
// the driver.
func (cfg *Config) FormatDSN() string {
	u := cfg.url(nil, true)
	query := u.Query()
	if cfg.Warehouse != "" {
		query.Set("warehouse", cfg.Warehouse)
	} else {
		panic("no warehouse")
	}
	if cfg.Org != "" {
		query.Set("org", cfg.Org)
	} else {
		panic(" no org")
	}
	if cfg.Timeout != 0 {
		query.Set("timeout", cfg.Timeout.String())
	}
	if cfg.IdleTimeout != 0 {
		query.Set("idle_timeout", cfg.IdleTimeout.String())
	}
	if cfg.ReadTimeout != 0 {
		query.Set("read_timeout", cfg.ReadTimeout.String())
	}
	if cfg.WriteTimeout != 0 {
		query.Set("write_timeout", cfg.WriteTimeout.String())
	}
	if cfg.Location != time.UTC && cfg.Location != nil {
		query.Set("location", cfg.Location.String())
	}
	if cfg.GzipCompression {
		query.Set("enable_http_compression", "1")
	}
	if cfg.Debug {
		query.Set("debug", "1")
	}
	if cfg.TLSConfig != "" {
		query.Set("tls_config", cfg.TLSConfig)
	}

	u.RawQuery = query.Encode()
	return u.String()
}

func (cfg *Config) url(extra map[string]string, dsn bool) *url.URL {
	u := &url.URL{
		Host:   ensureHavePort(cfg.Host),
		Scheme: cfg.Scheme,
		Path:   "/",
	}
	if len(cfg.User) > 0 {
		if len(cfg.Password) > 0 {
			u.User = url.UserPassword(cfg.User, cfg.Password)
		} else {
			u.User = url.User(cfg.User)
		}
	}
	query := u.Query()
	if len(cfg.Database) > 0 {
		if dsn {
			u.Path += cfg.Database
		} else {
			query.Set("database", cfg.Database)
		}
	}
	for k, v := range cfg.Params {
		query.Set(k, v)
	}
	for k, v := range extra {
		query.Set(k, v)
	}

	u.RawQuery = query.Encode()
	return u
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	cfg := NewConfig()

	cfg.Scheme, cfg.Host = u.Scheme, u.Host
	if len(u.Path) > 1 {
		// skip '/'
		cfg.Database = u.Path[1:]
	}
	if u.User != nil {
		// it is expected that empty password will be dropped out on Parse and Format
		cfg.User = u.User.Username()
		if passwd, ok := u.User.Password(); ok {
			cfg.Password = passwd
		}
	}
	if err = parseDSNParams(cfg, map[string][]string(u.Query())); err != nil {
		return nil, err
	}
	return cfg, nil
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params map[string][]string) (err error) {
	for k, v := range params {
		if len(v) == 0 {
			continue
		}

		switch k {
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(v[0])
		case "idle_timeout":
			cfg.IdleTimeout, err = time.ParseDuration(v[0])
		case "read_timeout":
			cfg.ReadTimeout, err = time.ParseDuration(v[0])
		case "write_timeout":
			cfg.WriteTimeout, err = time.ParseDuration(v[0])
		case "location":
			cfg.Location, err = time.LoadLocation(v[0])
		case "debug":
			cfg.Debug, err = strconv.ParseBool(v[0])
		case "default_format", "query", "database":
			err = fmt.Errorf("unknown option '%s'", k)
		case "enable_http_compression":
			cfg.GzipCompression, err = strconv.ParseBool(v[0])
			cfg.Params[k] = v[0]
		case "tls_config":
			cfg.TLSConfig = v[0]
		case "org":
			cfg.Org = v[0]
		case "warehouse":
			cfg.Warehouse = v[0]
		default:
			cfg.Params[k] = v[0]
		}
		if err != nil {
			return err
		}
	}

	return
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		// we get the missing port error here
		if addr[0] == '[' && addr[len(addr)-1] == ']' {
			// ipv6 brackets
			addr = addr[1 : len(addr)-1]
		}
		return net.JoinHostPort(addr, "443")
	}
	return addr
}