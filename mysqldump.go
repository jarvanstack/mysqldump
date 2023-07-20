package mysqldump

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	// 打印 日志 行数
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

var version string = "v0.11.0"

type dumpOption struct {
	// 导出表数据
	isData bool
	// 导出指定数据库, 与 WithAllDatabases 互斥, WithAllDatabases 优先级高
	dbs []string
	// 导出全部数据库
	isAllDB bool
	// 导出指定表, 与 isAllTables 互斥, isAllTables 优先级高
	tables []string
	// 导出全部表
	isAllTable bool
	// 是否删除表
	isDropTable bool
	// 是否增加选库脚本，多库导出时，此设置默认开启
	isUseDb bool

	//批量插入，提高导出效率
	perDataNumber int

	// writer 默认为 os.Stdout
	writer io.Writer
	//是否输出日志
	logOut bool
}
type triggerStruct struct {
	Trigger   string
	Event     string
	Table     string
	Statement string
	Timing    string
}

var allTriggers map[string][]triggerStruct

type DumpOption func(*dumpOption)

// 删除表
func WithDropTable() DumpOption {
	return func(option *dumpOption) {
		option.isDropTable = true
	}
}

// 导出表数据
func WithData() DumpOption {
	return func(option *dumpOption) {
		option.isData = true
	}
}

// 导出全部数据库
func WithAllDatabases() DumpOption {
	return func(option *dumpOption) {
		option.isAllDB = true
	}
}

// 是否增加指定库语句 如果多库，此设置无效
func WithUseDb() DumpOption {
	return func(option *dumpOption) {
		option.isUseDb = true
	}
}

// 导出指定数据库, 与 WithAllDatabases 互斥, WithAllDatabases 优先级高
func WithDBs(databases ...string) DumpOption {
	return func(option *dumpOption) {
		option.dbs = databases
	}
}

// 导出指定表, 与 WithAllTables 互斥, WithAllTables 优先级高
func WithTables(tables ...string) DumpOption {
	return func(option *dumpOption) {
		option.tables = tables
	}
}

// 导出全部表
func WithAllTable() DumpOption {
	return func(option *dumpOption) {
		option.isAllTable = true
	}
}

// 批量insert
func WithMultyInsert(num int) DumpOption {
	return func(option *dumpOption) {
		option.perDataNumber = num
	}
}

// 导出到指定 writer
func WithWriter(writer io.Writer) DumpOption {
	return func(option *dumpOption) {
		option.writer = writer
	}
}

// 是否输出日志
// @TODO: 后续增加日志的handle用于输出到其他地方
func WithLogOut(logOut bool) DumpOption {
	return func(option *dumpOption) {
		option.logOut = logOut
	}
}

func Dump(dns string, opts ...DumpOption) error {

	var err error

	var o dumpOption
	// 打印开始
	start := time.Now()
	if o.logOut {
		log.Printf("[info] [dump] start at %s\n", start.Format("2006-01-02 15:04:05"))
	}

	// 打印结束
	defer func() {
		end := time.Now()
		if o.logOut {
			log.Printf("[info] [dump] end at %s, cost %s\n", end.Format("2006-01-02 15:04:05"), end.Sub(start))
		}
	}()

	for _, opt := range opts {
		opt(&o)
	}

	if len(o.dbs) == 0 {
		// 默认包含dns中的数据库
		dbName, err := GetDBNameFromDNS(dns)
		if err != nil {
			log.Printf("[error] %v \n", err)
			return err
		}
		o.dbs = []string{
			dbName,
		}
	}
	if len(o.tables) == 0 {
		// 默认包含全部表
		o.isAllTable = true
	}

	if o.writer == nil {
		// 默认输出到 os.Stdout
		o.writer = os.Stdout
	}

	buf := bufio.NewWriter(o.writer)
	defer buf.Flush()

	// 打印 Header
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString("-- MySQL Database Dump\n")
	buf.WriteString("-- GoMysqlDump version: " + version + "\n")
	buf.WriteString("-- Start Time: " + start.Format("2006-01-02 15:04:05") + "\n")
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString("\n\n")
	buf.WriteString("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n")
	// 连接数据库
	db, err := sql.Open("mysql", dns)
	if err != nil {
		if o.logOut {
			log.Printf("[error] %v \n", err)
		}
		return err
	}
	defer db.Close()

	// 1. 获取数据库
	var dbs []string
	if o.isAllDB {
		dbs, err = getDBs(db)
		if err != nil {
			if o.logOut {
				log.Printf("[error] %v \n", err)
			}
			return err
		}
	} else {
		dbs = o.dbs
	}
	if len(dbs) > 1 {
		o.isUseDb = true
	}
	// 2. 获取表
	for _, dbStr := range dbs {
		_, err = db.Exec(fmt.Sprintf("USE `%s`", dbStr))
		if err != nil {
			if o.logOut {
				log.Printf("[error] %v \n", err)
			}
			return err
		}

		var tables []string
		if o.isAllTable {
			tmp, err := getAllTables(db)
			if err != nil {
				if o.logOut {
					log.Printf("[error] %v \n", err)
				}
				return err
			}
			tables = tmp
		} else {
			tables = o.tables
		}
		if o.isUseDb {
			//多库导出时，才会增加选库操作，否则不加选库操作
			buf.WriteString(fmt.Sprintf("USE `%s`;\n", dbStr))
		}

		// 3. 导出表
		for _, table := range tables {

			tt, err := getTableType(db, table)
			if err != nil {
				return err
			}

			if tt == "TABLE" {
				// 删除表
				if o.isDropTable {
					buf.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table))
				}

				// 导出表结构
				err = writeTableStruct(db, table, buf)
				if err != nil {
					if o.logOut {
						log.Printf("[error] %v \n", err)
					}
					return err
				}
				// 导出表数据
				if o.isData {
					err = writeTableData(db, table, buf, o.perDataNumber)
					if err != nil {
						if o.logOut {
							log.Printf("[error] %v \n", err)
						}
						return err
					}
				}
				err := writeTableTrigger(db, table, buf)
				if err != nil {
					if o.logOut {
						log.Printf("[error] %v \n", err)
					}
					return err
				}
			}
			if tt == "VIEW" {
				// 删除视图
				if o.isDropTable {
					buf.WriteString(fmt.Sprintf("DROP VIEW IF EXISTS  `%s`;\n", table))
				}
				// 导出视图结构
				err = writeViewStruct(db, table, buf)
				if err != nil {
					if o.logOut {
						log.Printf("[error] %v \n", err)
					}
					return err
				}
			}

		}

	}

	// 导出每个表的结构和数据

	buf.WriteString("-- ----------------------------\n")
	buf.WriteString("-- Dumped by mysqldump2\n")
	buf.WriteString("-- Cost Time: " + time.Since(start).String() + "\n")
	buf.WriteString("-- ----------------------------\n")
	buf.Flush()

	return nil
}
func getTableType(db *sql.DB, table string) (t string, err error) {
	query := fmt.Sprintf("SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s'", table)
	var tableType string
	err = db.QueryRow(query).Scan(&tableType)
	if err != nil {
		return "", err
	}
	switch tableType {
	case "BASE TABLE":
		return "TABLE", nil
	case "VIEW":
		return "VIEW", nil
	default:
		return "", nil
	}
}

func getCreateTableSQL(db *sql.DB, table string) (string, error) {

	var createTableSQL string

	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&table, &createTableSQL)
	if err != nil {
		return "", err
	}
	// IF NOT EXISTS
	createTableSQL = strings.Replace(createTableSQL, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	return createTableSQL, nil
}

func getDBs(db *sql.DB) ([]string, error) {
	var dbs []string
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var db string
		err = rows.Scan(&db)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	return dbs, nil
}

func getAllTables(db *sql.DB) ([]string, error) {
	var tables []string
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func writeTableStruct(db *sql.DB, table string, buf *bufio.Writer) error {
	// 导出表结构
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("-- Table structure for %s\n", table))
	buf.WriteString("-- ----------------------------\n")

	createTableSQL, err := getCreateTableSQL(db, table)
	if err != nil {
		return err
	}
	buf.WriteString(createTableSQL)
	buf.WriteString(";")

	buf.WriteString("\n\n")
	buf.WriteString("\n\n")
	return nil
}

func writeViewStruct(db *sql.DB, table string, buf *bufio.Writer) error {
	// 导出视图
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("-- View structure for %s\n", table))
	buf.WriteString("-- ----------------------------\n")

	var createTableSQL string
	var charact string
	var connect string
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&table, &createTableSQL, &charact, &connect)
	if err != nil {
		return err
	}
	buf.WriteString(createTableSQL)
	buf.WriteString(";")

	buf.WriteString("\n\n")
	buf.WriteString("\n\n")
	return nil
}

func writeTableData(db *sql.DB, table string, buf *bufio.Writer, perDataNumber int) error {

	// 导出表数据
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("-- Records of %s\n", table))
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("LOCK TABLES `%s` WRITE;\n", table))
	buf.WriteString(fmt.Sprintf("/*!40000 ALTER TABLE `%s` DISABLE KEYS */;\n", table))

	lineRows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return err
	}
	defer lineRows.Close()

	var columns []string
	columns, err = lineRows.Columns()
	if err != nil {
		return err
	}
	columnTypes, err := lineRows.ColumnTypes()
	if err != nil {
		return err
	}

	var values [][]interface{}
	rowId := 0

	for lineRows.Next() {
		ssql := ""
		if rowId == 0 || perDataNumber < 2 || rowId%perDataNumber == 0 {
			if rowId > 0 {
				ssql = ";\n"
			}
			//表结构
			ssql += "INSERT INTO `" + table + "` (`" + strings.Join(columns, "`,`") + "`) VALUES \n"
		} else {
			buf.WriteString(",\n")
		}

		row := make([]interface{}, len(columns))
		rowPointers := make([]interface{}, len(columns))
		for i := range columns {
			rowPointers[i] = &row[i]
		}
		err = lineRows.Scan(rowPointers...)
		if err != nil {
			return err
		}
		rowString, err := buildRowData(row, columnTypes)
		if err != nil {
			return err
		}
		ssql += "(" + rowString + ")"
		rowId += 1
		buf.WriteString(ssql)
		values = append(values, row)
	}
	buf.WriteString(";\n")
	buf.WriteString(fmt.Sprintf("/*!40000 ALTER TABLE `%s` ENABLE KEYS */;\n", table))
	buf.WriteString("UNLOCK TABLES;\n\n")
	return nil

}

func buildRowData(row []interface{}, columnTypes []*sql.ColumnType) (ssql string, err error) {
	// var ssql string
	for i, col := range row {
		if col == nil {
			ssql += "NULL"
		} else {
			Type := columnTypes[i].DatabaseTypeName()
			// 去除 UNSIGNED 和空格
			Type = strings.Replace(Type, "UNSIGNED", "", -1)
			Type = strings.Replace(Type, " ", "", -1)
			switch Type {
			case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
				if bs, ok := col.([]byte); ok {
					ssql += fmt.Sprintf("%s", string(bs))
				} else {
					ssql += fmt.Sprintf("%d", col)
				}
			case "FLOAT", "DOUBLE":
				if bs, ok := col.([]byte); ok {
					ssql += fmt.Sprintf("%s", string(bs))
				} else {
					ssql += fmt.Sprintf("%f", col)
				}
			case "DECIMAL", "DEC":
				ssql += fmt.Sprintf("%s", col)

			case "DATE":
				t, ok := col.(time.Time)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("'%s'", t.Format("2006-01-02"))
			case "DATETIME":
				t, ok := col.(time.Time)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05"))
			case "TIMESTAMP":
				t, ok := col.(time.Time)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05"))
			case "TIME":
				t, ok := col.([]byte)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("'%s'", string(t))
			case "YEAR":
				t, ok := col.([]byte)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("%s", string(t))
			case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
				r := strings.NewReplacer("\n", "\\n", "'", "\\'", "\r", "\\r", "\"", "\\\"")
				ssql += fmt.Sprintf("'%s'", r.Replace(fmt.Sprintf("%s", col)))
				// ssql += fmt.Sprintf("'%s'", strings.Replace(fmt.Sprintf("%s", col), "'", "''", -1))
			case "BIT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
				ssql += fmt.Sprintf("0x%X", col)
			case "ENUM", "SET":
				ssql += fmt.Sprintf("'%s'", col)
			case "BOOL", "BOOLEAN":
				if col.(bool) {
					ssql += "true"
				} else {
					ssql += "false"
				}
			case "JSON":
				ssql += fmt.Sprintf("'%s'", col)
			default:
				// unsupported type
				return "", fmt.Errorf("unsupported type: %s", Type)
			}
		}
		if i < len(row)-1 {
			ssql += ","
		}
	}
	return ssql, nil
}

func writeTableTrigger(db *sql.DB, table string, buf *bufio.Writer) error {
	var sql []string

	triggers, err := getTrigger(db, table)
	if err != nil {
		return err
	}
	if len(triggers) > 0 {
		sql = append(sql, "-- ----------------------------")
		sql = append(sql, fmt.Sprintf("-- Dump table triggers of %s--------", table))
		sql = append(sql, "-- ----------------------------")
	}
	for _, v := range triggers {
		sql = append(sql, "DELIMITER ;;")
		sql = append(sql, "/*!50003 SET SESSION SQL_MODE=\"\" */;;")
		sql = append(sql, fmt.Sprintf("/*!50003 CREATE TRIGGER `%s` %s %s ON `%s` FOR EACH ROW %s */;;", v.Trigger, v.Timing, v.Event, v.Table, v.Statement))
		sql = append(sql, "DELIMITER ;")
		sql = append(sql, "/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;\n")
	}
	buf.WriteString(strings.Join(sql, "\n"))
	return nil
}

func getTrigger(db *sql.DB, table string) (trigger []triggerStruct, err error) {
	if allTriggers != nil {
		trigger = allTriggers[table]
		return trigger, nil
	} else {
		allTriggers = make(map[string][]triggerStruct)
	}
	trgs, err := db.Query("SHOW TRIGGERS")
	if err != nil {
		return trigger, err
	}
	defer trgs.Close()

	var columns []string
	columns, err = trgs.Columns()

	for trgs.Next() {
		trgrow := make([]interface{}, len(columns))
		rowPointers := make([]interface{}, len(columns))
		for i := range columns {
			rowPointers[i] = &trgrow[i]
		}
		err = trgs.Scan(rowPointers...)
		if err != nil {
			return trigger, err
		}
		var trigger triggerStruct
		for k, v := range trgrow {
			switch columns[k] {
			case "Table":
				trigger.Table = fmt.Sprintf("%s", v)
			case "Event":
				trigger.Event = fmt.Sprintf("%s", v)
			case "Trigger":
				trigger.Trigger = fmt.Sprintf("%s", v)
			case "Statement":
				trigger.Statement = fmt.Sprintf("%s", v)
			case "Timing":
				trigger.Timing = fmt.Sprintf("%s", v)
			}
		}
		allTriggers[trigger.Table] = append(allTriggers[trigger.Table], trigger)
	}
	return allTriggers[table], nil
}
