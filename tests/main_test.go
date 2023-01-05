package tests

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/test-go/testify/suite"

	dc "github.com/databendcloud/databend-go"
)

const (
	createTable = `CREATE TABLE %s (
		i64 Int64,
		u64 UInt64,
		f64 Float64,
		s   String,
		s2  String,
		a16 Array(Int16),
		a8  Array(UInt8),
		d   Date,
		t   DateTime)`
)

func TestDatabendSuite(t *testing.T) {
	suite.Run(t, new(DatabendTestSuite))
}

type DatabendTestSuite struct {
	suite.Suite
	db    *sql.DB
	table string
	r     *require.Assertions
}

func (s *DatabendTestSuite) SetupSuite() {
	var err error

	dsn := os.Getenv("TEST_DATABEND_DSN")
	s.NotEmpty(dsn)

	s.db, err = sql.Open("databend", dsn)
	s.Nil(err)

	err = s.db.Ping()
	s.Nil(err)

	var version string
	err = s.db.QueryRow("select version()").Scan(&version)
	s.Nil(err)

	s.T().Logf("connected to databend: %s\n", version)
}

func (s *DatabendTestSuite) TearDownSuite() {
	s.db.Close()
}

func (s *DatabendTestSuite) SetupTest() {
	t := s.T()
	s.r = require.New(t)

	s.table = fmt.Sprintf("test_%s_%d", t.Name(), time.Now().Unix())
	// t.Logf("setup test with table %s", s.table)

	_, err := s.db.Exec(fmt.Sprintf(createTable, s.table))
	s.r.Nil(err)
}

func (s *DatabendTestSuite) TearDownTest() {
	// t := s.T()

	// t.Logf("teardown test with table %s", s.table)
	_, err := s.db.Exec(fmt.Sprintf("DROP TABLE %s", s.table))
	s.r.Nil(err)
}

func (s *DatabendTestSuite) TestDesc() {
	rows, err := s.db.Query("DESC " + s.table)
	s.r.Nil(err)

	result, err := scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{
		{"i64", "BIGINT", "NO", "0", ""},
		{"u64", "BIGINT UNSIGNED", "NO", "0", ""},
		{"f64", "DOUBLE", "NO", "0", ""},
		{"s", "VARCHAR", "NO", "", ""},
		{"s2", "VARCHAR", "NO", "", ""},
		{"a16", "ARRAY(INT16)", "NO", "[]", ""},
		{"a8", "ARRAY(UINT8)", "NO", "[]", ""},
		{"d", "DATE", "NO", "0", ""},
		{"t", "TIMESTAMP", "NO", "0", ""},
	}, result)
	rows.Close()
}

func (s *DatabendTestSuite) TestBasicSelect() {
	query := "SELECT ?"
	result, err := s.db.Exec(query, []interface{}{1}...)
	s.r.Nil(err)

	affected, err := result.RowsAffected()
	s.r.Nil(err)
	// s.r.ErrorIs(err, dc.ErrNoRowsAffected)
	s.r.Equal(int64(0), affected)
}

func (s *DatabendTestSuite) TestBatchInsert() {
	r := require.New(s.T())

	scope, err := s.db.Begin()
	r.Nil(err)

	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s VALUES", s.table))
	r.Nil(err)

	for i := 0; i < 10; i++ {
		_, err = batch.Exec(
			"1234",
			"2345",
			"3.1415",
			"test",
			"test2",
			"[4, 5, 6]",
			"[1, 2, 3]",
			"2021-01-01",
			"2021-01-01 00:00:00",
		)
		r.Nil(err)
	}

	err = scope.Commit()
	r.Nil(err)
}

func (s *DatabendTestSuite) TestDDL() {
	ddls := []string{
		`DROP TABLE IF EXISTS data`,
		`CREATE TABLE data (
			i64 Int64,
			u64 UInt64,
			f64 Float64,
			s   String,
			s2  String,
			a16 Array(Int16),
			a8  Array(UInt8),
			d   Date,
			t   DateTime)
	`,
		`INSERT INTO data VALUES
		(-1, 1, 1.0, '1', '1', [1], [10], '2011-03-06', '2011-03-06 06:20:00'),
		(-2, 2, 2.0, '2', '2', [2], [20], '2012-05-31', '2012-05-31 11:20:00'),
		(-3, 3, 3.0, '3', '2', [3], [30], '2016-04-04', '2016-04-04 11:30:00')
	`,
	}
	for _, ddl := range ddls {
		_, err := s.db.Exec(ddl)
		s.Nil(err)
	}
}

func (s *DatabendTestSuite) TestExec() {
	testCases := []struct {
		query  string
		query2 string
		args   []interface{}
	}{
		{
			fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table),
			fmt.Sprintf("SELECT i64 FROM %s WHERE i64=?", s.table),
			[]interface{}{int64(1)},
		},
		{
			fmt.Sprintf("INSERT INTO %s (i64, u64) VALUES (?, ?)", s.table),
			fmt.Sprintf("SELECT i64, u64 FROM %s WHERE i64=? AND u64=?", s.table),
			[]interface{}{int64(2), uint64(12)},
		},
		{
			fmt.Sprintf("INSERT INTO %s (i64, a16, a8) VALUES (?, ?, ?)", s.table),
			"",
			[]interface{}{int64(3), dc.Array([]int16{1, 2}), dc.Array([]uint8{10, 20})},
		},
		{
			fmt.Sprintf("INSERT INTO %s (d, t) VALUES (?, ?)", s.table),
			"",
			[]interface{}{
				dc.Date(time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local)),
				time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local),
			},
		},
	}
	for _, tc := range testCases {
		result, err := s.db.Exec(tc.query, tc.args...)
		s.T().Logf("query: %s, args: %v\n", tc.query, tc.args)
		s.r.Nil(err)
		s.r.NotNil(result)

		if len(tc.query2) == 0 {
			continue
		}
		rows, err := s.db.Query(tc.query2, tc.args...)
		s.r.Nil(err)

		v, err := scanValues(rows)
		s.r.Nil(err)
		s.r.Equal([][]interface{}{tc.args}, v)

		s.r.NoError(rows.Close())
	}
}

func (s *DatabendTestSuite) TestServerError() {
	_, err := s.db.Query("SELECT 1 FROM '???'")
	s.Contains(err.Error(), "error")
}

func scanValues(rows *sql.Rows) (interface{}, error) {
	var err error
	var result [][]interface{}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	types := make([]reflect.Type, len(ct))
	for i, v := range ct {
		types[i] = v.ScanType()
	}
	ptrs := make([]interface{}, len(types))
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		for i, t := range types {
			ptrs[i] = reflect.New(t).Interface()
		}
		err = rows.Scan(ptrs...)
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, len(types))
		for i, p := range ptrs {
			values[i] = reflect.ValueOf(p).Elem().Interface()
		}
		result = append(result, values)
	}
	return result, nil
}
