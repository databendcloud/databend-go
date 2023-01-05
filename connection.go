package godatabend

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go"
)

const (
	accept          = "Accept"
	authorization   = "Authorization"
	contentType     = "Content-Type"
	jsonContentType = "application/json; charset=utf-8"
	timeZone        = "Time-Zone"
	userAgent       = "User-Agent"
)

type DatabendConn struct {
	url                *url.URL
	ctx                context.Context
	cfg                *Config
	SQLState           string
	transport          *http.Transport
	cancel             context.CancelFunc
	closed             int32
	stmts              []*databendStmt
	txCtx              context.Context
	useDBLocation      bool
	useGzipCompression bool
	killQueryOnErr     bool
	killQueryTimeout   time.Duration
	logger             *log.Logger
	rest               *APIClient
	commit             func() error
}

func (dc *DatabendConn) exec(ctx context.Context, query string, args ...driver.Value) (driver.Result, error) {
	respCh := make(chan QueryResponse)
	errCh := make(chan error)
	go func() {
		err := dc.rest.QuerySync(ctx, query, "", args, respCh)
		errCh <- err
	}()

	for {
		select {
		case err := <-errCh:
			if err != nil {
				return emptyResult, err
			} else {
				return emptyResult, nil
			}
		case resp := <-respCh:
			b, err := json.Marshal(resp.Data)
			if err != nil {
				return emptyResult, err
			}
			_, _ = io.Copy(io.Discard, bytes.NewReader(b))
		}
	}
}

func (dc *DatabendConn) query(ctx context.Context, query string, args ...driver.Value) (driver.Rows, error) {
	var r0 *QueryResponse
	err := retry.Do(
		func() error {
			r, err := dc.rest.DoQuery(ctx, query, args)
			if err != nil {
				return err
			}
			r0 = r
			return nil
		},
		// other err no need to retry
		retry.RetryIf(func(err error) bool {
			if err != nil && (IsProxyErr(err) || strings.Contains(err.Error(), ProvisionWarehouseTimeout)) {
				return true
			}
			return false
		}),
		retry.Delay(2*time.Second),
		retry.Attempts(5),
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	if r0.Error != nil {
		return nil, fmt.Errorf("query has error: %+v", r0.Error)
	}
	return newNextRows(dc, r0)
}

//func (dc *DatabendConn) Begin() (driver.Tx, error) {
//	return dc.BeginTx(dc.ctx, driver.TxOptions{})
//}

func (dc *DatabendConn) Begin() (driver.Tx, error) { return dc, nil }

func (dc *DatabendConn) cleanup() {
	// must flush log buffer while the process is running.
	dc.rest = nil
	dc.cfg = nil
}

func (dc *DatabendConn) Prepare(query string) (driver.Stmt, error) {
	return dc.PrepareContext(context.Background(), query)
}

func (dc *DatabendConn) prepare(query string) (*databendStmt, error) {
	logger.WithContext(dc.ctx).Infoln("Prepare")
	if dc.rest == nil {
		return nil, driver.ErrBadConn
	}
	batch, err := dc.prepareBatch(dc.ctx, query)
	if err != nil {
		return nil, err
	}
	dc.commit = batch.InsertIntoWithStage

	stmt := &databendStmt{
		dc:    dc,
		query: query,
		batch: batch,
	}
	return stmt, nil
}

func (dc *DatabendConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return dc.prepare(query)
}

func buildDatabendConn(ctx context.Context, config Config) (*DatabendConn, error) {
	dc := &DatabendConn{
		url: config.url(map[string]string{"default_format": "TabSeparatedWithNamesAndTypes"}),
		ctx: ctx,
		cfg: &config,
		transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   config.Timeout,
				KeepAlive: config.IdleTimeout,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          1,
			IdleConnTimeout:       config.IdleTimeout,
			ResponseHeaderTimeout: config.ReadTimeout,
			TLSClientConfig:       getTLSConfigClone(config.TLSConfig),
		},
	}
	if config.Debug {
		dc.logger = log.New(os.Stderr, "databend: ", log.LstdFlags)
	}
	var apiScheme string
	switch dc.cfg.SSLMode {
	case SSL_MODE_DISABLE:
		apiScheme = "http"
	default:
		apiScheme = "https"
	}
	dc.rest = &APIClient{
		ApiEndpoint: fmt.Sprintf("%s://%s", apiScheme, dc.cfg.Host),
		Host:        dc.cfg.Host,
		Tenant:      dc.cfg.Tenant,
		Warehouse:   dc.cfg.Warehouse,
		User:        dc.cfg.User,
		Password:    dc.cfg.Password,
		AccessToken: dc.cfg.AccessToken,

		PresignedURLDisabled: dc.cfg.PresignedURLDisabled,
	}
	return dc, nil
}

func (dc *DatabendConn) log(msg ...interface{}) {
	if dc.logger != nil {
		dc.logger.Println(msg...)
	}
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (dc *DatabendConn) Close() error {
	if atomic.CompareAndSwapInt32(&dc.closed, 0, 1) {
		dc.log("close connection", dc.url.Scheme, dc.url.Host, dc.url.Path)
		cancel := dc.cancel
		transport := dc.transport
		dc.transport = nil
		dc.cancel = nil

		if cancel != nil {
			cancel()
		}
		if transport != nil {
			transport.CloseIdleConnections()
		}
		dc.cleanup()
	}
	return nil
}

func (dc *DatabendConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return dc.exec(context.Background(), query, args...)
}

func (dc *DatabendConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return dc.exec(ctx, query, values...)
}

func (dc *DatabendConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return dc.query(context.Background(), query, args...)
}

func (dc *DatabendConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return dc.query(ctx, query, values...)
}

// Commit applies prepared statement if it exists
func (dc *DatabendConn) Commit() (err error) {
	if dc.commit == nil {
		return nil
	}
	defer func() {
		dc.commit = nil
	}()
	return dc.commit()
}

// Rollback cleans prepared statement
func (dc *DatabendConn) Rollback() error {
	dc.commit = nil
	dc.Close()
	return nil
}
