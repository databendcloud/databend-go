package godatabend

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	ldriver "github.com/databendcloud/databend-go/lib/driver"
)

// \x60 represents a backtick
var httpInsertRe = regexp.MustCompile(`(?i)^INSERT INTO\s+\x60?([\w.^\(]+)\x60?\s*(\([^\)]*\))?`)

func (dc *DatabendConn) prepareBatch(ctx context.Context, query string) (ldriver.Batch, error) {
	matches := httpInsertRe.FindStringSubmatch(query)
	if len(matches) < 3 {
		return nil, errors.New("cannot get table name from query")
	}
	tableName := matches[1]
	var rColumns []string
	if matches[2] != "" {
		colMatch := strings.TrimSuffix(strings.TrimPrefix(matches[2], "("), ")")
		rColumns = strings.Split(colMatch, ",")
		for i := range rColumns {
			rColumns[i] = strings.TrimSpace(rColumns[i])
		}
	}
	queryTableSchema := "DESCRIBE " + tableName

	r, err := dc.rest.DoQuery(ctx, queryTableSchema, nil)
	if err != nil {
		return nil, err
	}
	// get Table columns and types
	var columnNames, columnTypes []string
	for i := range r.Data {
		if len(r.Data[i]) > 1 {
			columnNames = append(columnNames, fmt.Sprintf("%s", r.Data[i][0]))
			columnTypes = append(columnTypes, fmt.Sprintf("%s", r.Data[i][1]))
		}
	}

	csvFileName := fmt.Sprintf("%s.csv", uuid.NewString())

	csvFile, err := os.OpenFile(csvFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	err = writer.Write(columnNames)
	if err != nil {
		return nil, err
	}
	writer.Flush()

	return &httpBatch{
		query:       query,
		ctx:         ctx,
		conn:        dc,
		columnNames: columnNames,
		columnTypes: columnTypes,
		tableSchema: tableName,
		batchFile:   csvFileName,
	}, nil
}

type httpBatch struct {
	query       string
	err         error
	ctx         context.Context
	conn        *DatabendConn
	columnNames []string
	columnTypes []string
	batchFile   string
	tableSchema string
}

func (b *httpBatch) InsertIntoWithStage() error {
	defer func() {
		err := os.RemoveAll(b.batchFile)
		if err != nil {
			b.conn.log("delete batch insert file failed: ", err)
		}
	}()
	b.conn.log("upload to stage")
	err := b.UploadToStage()
	if err != nil {
		return errors.Wrap(err, "upload to stage failed")
	}

	// insert into db.table from @~/xx.csv
	stageFile := fmt.Sprintf("@~/%s", b.batchFile)
	respCh := make(chan QueryResponse)
	errCh := make(chan error)
	go func() {
		err := b.conn.rest.QuerySync(context.Background(), b.query, stageFile, nil, respCh)
		errCh <- err
	}()

	for {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			} else {
				return nil
			}
		case resp := <-respCh:
			bt, err := json.Marshal(resp.Data)
			if err != nil {
				return err
			}
			_, _ = io.Copy(io.Discard, bytes.NewReader(bt))
		}
	}
}

func (b *httpBatch) CopyInto() error {
	defer func() {
		err := os.RemoveAll(b.batchFile)
		if err != nil {
			b.conn.log("delete batch insert file failed: ", err)
		}
	}()
	b.conn.log("upload to stage")
	err := b.UploadToStage()
	if err != nil {
		return errors.Wrap(err, "upload to stage failed")
	}

	// copy into db.table from @~/xx.csv
	respCh := make(chan QueryResponse)
	errCh := make(chan error)
	go func() {
		err := b.conn.rest.QuerySync(context.Background(), newCopyInto(b.tableSchema, b.batchFile), "", nil, respCh)
		errCh <- err
	}()

	for {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			} else {
				return nil
			}
		case resp := <-respCh:
			bt, err := json.Marshal(resp.Data)
			if err != nil {
				return err
			}
			_, _ = io.Copy(io.Discard, bytes.NewReader(bt))
		}
	}
}

func newCopyInto(tableSchema, fileName string) string {
	return fmt.Sprintf("COPY INTO %s FROM @%s files=('%s') file_format = (type = 'CSV' field_delimiter = ','  record_delimiter = '\\n' skip_header = 1);", tableSchema, "~", fileName)
}

func (b *httpBatch) AppendToFile(v []driver.Value) error {
	csvFile, err := os.OpenFile(b.batchFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	lineData := make([]string, 0, len(v))
	for i := range v {
		lineData = append(lineData, fmt.Sprintf("%v", v[i]))
	}
	// fmt.Printf("%v", lineData)
	writer := csv.NewWriter(csvFile)
	err = writer.Write(lineData)
	if err != nil {
		return err
	}
	writer.Flush()

	return nil
}

func (b *httpBatch) UploadToStage() error {
	return b.conn.rest.uploadToStage(b.batchFile)
}

var _ ldriver.Batch = (*httpBatch)(nil)
