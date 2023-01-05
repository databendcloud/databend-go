package godatabend

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"

	ldriver "github.com/databendcloud/databend-go/lib/driver"
	"github.com/pkg/errors"
)

var (
	splitInsertRe = regexp.MustCompile(`(?si)(.+\s*VALUES)\s*(\(.+\))`)
)

type databendStmt struct {
	dc        *DatabendConn
	closed    int32
	prefix    string
	pattern   string
	index     []int
	batchMode bool
	args      [][]driver.Value
	query     string
	batch     ldriver.Batch
}

func (stmt *databendStmt) Close() error {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.Close")
	return nil
}

func (stmt *databendStmt) NumInput() int {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.NumInput")
	return -1
}

func (stmt *databendStmt) Exec(args []driver.Value) (driver.Result, error) {
	//1. trans args to csv file
	err := stmt.batch.AppendToFile(args)
	if err != nil {
		return nil, err
	}

	//2. /v1/upload_to_stage csv file

	// 3. copy into db.table from @~/csv

	// 4. delete the file ?

	return driver.RowsAffected(0), nil
}

//func (stmt *databendStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
//	values := make([]driver.Value, 0, len(args))
//	for _, v := range args {
//		values = append(values, v.Value)
//	}
//	return stmt.Exec(values)
//}

func (stmt *databendStmt) Query(args []driver.Value) (driver.Rows, error) {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.Query")
	return nil, errors.New("only Exec method supported in batch mode")
}

func (stmt *databendStmt) commit(ctx context.Context) error {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt Commit")
	err := stmt.batch.UploadToStage()
	if err != nil {
		fmt.Printf("upload stage failed %v", err)
	}

	return stmt.batch.InsertIntoWithStage()
}
