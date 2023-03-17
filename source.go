package mysqldump

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
)

type sourceOption struct {
	dryRun       bool
	isDeleteDB   bool
	isExecByLine bool
}
type SourceOption func(*sourceOption)

func WithDryRun() SourceOption {
	return func(o *sourceOption) {
		o.dryRun = true
	}
}

func WithDeleteDB() SourceOption {
	return func(o *sourceOption) {
		o.isDeleteDB = true
	}
}

// 一句一句执行
func WithExecByLine() SourceOption {
	return func(o *sourceOption) {
		o.isExecByLine = true
	}
}

type DBWrapper struct {
	DB     *sql.DB
	dryRun bool
}

func NewDBWrapper(db *sql.DB, dryRun bool) *DBWrapper {

	return &DBWrapper{
		DB:     db,
		dryRun: dryRun,
	}
}

func (db *DBWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	fmt.Printf("[SQL] query: %s , args: %v \n", query, args)
	if db.dryRun {
		return nil, nil
	}
	return db.DB.Exec(query, args...)
}

// Source 加载
func Source(dns string, reader io.Reader, opts ...SourceOption) error {
	var err error
	var db *sql.DB

	var o sourceOption
	for _, opt := range opts {
		opt(&o)
	}

	dbName, err := GetDBNameFromDNS(dns)
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	// 如果数据库不存在, 必须先创建数据库, 所以这里要把数据库名替换成mysql, 才能连接到mysql数据库
	mysqlDNS := strings.Replace(dns, dbName, "mysql", 1)

	// Open database
	db, err = sql.Open("mysql", mysqlDNS)
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}
	defer db.Close()

	// DB Wrapper
	dbWrapper := NewDBWrapper(db, o.dryRun)

	// 删除数据库
	if o.isDeleteDB {
		_, err = dbWrapper.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			log.Printf("[error] %v\n", err)
			return err
		}
	}

	// 创建数据库
	_, err = dbWrapper.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	// Use database
	_, err = dbWrapper.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	// 一句一句执行
	r := bufio.NewReader(reader)
	if o.isExecByLine {
		for {
			line, err := r.ReadString(';')
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Printf("[error] %v\n", err)
				return err
			}

			_, err = dbWrapper.Exec(string(line))
			if err != nil {
				log.Printf("[error] %v\n", err)
				return err
			}
		}
	} else {
		// 一次性执行
		all, err := io.ReadAll(r)
		if err != nil {
			log.Printf("[error] %v\n", err)
			return err
		}
		_, err = dbWrapper.Exec(string(all))
		if err != nil {
			log.Printf("[error] %v\n", err)
			return err
		}

	}
	return nil
}
