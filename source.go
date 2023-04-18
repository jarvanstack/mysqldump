package mysqldump

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
)

type sourceOption struct {
	dryRun      bool
	mergeInsert int
}
type SourceOption func(*sourceOption)

func WithDryRun() SourceOption {
	return func(o *sourceOption) {
		o.dryRun = true
	}
}

func WithMergeInsert(size int) SourceOption {
	return func(o *sourceOption) {
		o.mergeInsert = size
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

	// Open database
	db, err = sql.Open("mysql", dns)
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}
	defer db.Close()

	// DB Wrapper
	dbWrapper := NewDBWrapper(db, o.dryRun)

	// Use database
	_, err = dbWrapper.Exec(fmt.Sprintf("USE %s;", dbName))
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	// 设置超时时间1小时
	db.SetConnMaxLifetime(3600)

	// 一句一句执行
	r := bufio.NewReader(reader)
	// 关闭事务
	_, err = dbWrapper.Exec("SET autocommit=0;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	for {
		line, err := r.ReadString(';')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("[error] %v\n", err)
			return err
		}

		ssql := string(line)

		// 删除末尾的换行符
		ssql, err = trim(ssql)
		if err != nil {
			log.Printf("[error] [trim] %v\n", err)
			return err
		}

		// 如果 INSERT 开始, 并且 mergeInsert 为 true, 则合并 INSERT
		if o.mergeInsert > 1 && strings.HasPrefix(ssql, "INSERT INTO") {
			var insertSQLs []string
			insertSQLs = append(insertSQLs, ssql)
			for i := 0; i < o.mergeInsert-1; i++ {
				line, err := r.ReadString(';')
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Printf("[error] %v\n", err)
					return err
				}

				ssql2 := string(line)
				ssql2, err = trim(ssql2)
				if err != nil {
					log.Printf("[error] [trim] %v\n", err)
					return err
				}
				if strings.HasPrefix(ssql2, "INSERT INTO") {
					insertSQLs = append(insertSQLs, ssql2)
					continue
				}

				break
			}
			// 合并 INSERT
			ssql, err = mergeInsert(insertSQLs)
			if err != nil {
				log.Printf("[error] [mergeInsert] %v\n", err)
				return err
			}
		}

		_, err = dbWrapper.Exec(ssql)
		if err != nil {
			log.Printf("[error] %v\n", err)
			return err
		}
	}

	// 提交事务
	_, err = dbWrapper.Exec("COMMIT;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	// 开启事务
	_, err = dbWrapper.Exec("SET autocommit=1;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	return nil
}

/*
将多个 INSERT 合并为一个
输入:
INSERT INTO `test` VALUES (1, 'a');
INSERT INTO `test` VALUES (2, 'b');
输出
INSERT INTO `test` VALUES (1, 'a'), (2, 'b');
*/
func mergeInsert(insertSQLs []string) (string, error) {
	if len(insertSQLs) == 0 {
		return "", errors.New("no input provided")
	}
	builder := strings.Builder{}
	sql1 := insertSQLs[0]
	sql1 = strings.TrimSuffix(sql1, ";")
	builder.WriteString(sql1)
	for i, insertSQL := range insertSQLs[1:] {
		if i < len(insertSQLs)-1 {
			builder.WriteString(",")
		}

		valuesIdx := strings.Index(insertSQL, "VALUES")
		if valuesIdx == -1 {
			return "", errors.New("invalid SQL: missing VALUES keyword")
		}
		sqln := insertSQL[valuesIdx:]
		sqln = strings.TrimPrefix(sqln, "VALUES")
		sqln = strings.TrimSuffix(sqln, ";")
		builder.WriteString(sqln)

	}
	builder.WriteString(";")

	return builder.String(), nil
}

// 删除空白符换行符和注释
func trim(s string) (string, error) {
	s = strings.TrimLeft(s, "\n")
	s = strings.TrimSpace(s)
	return s, nil
}
