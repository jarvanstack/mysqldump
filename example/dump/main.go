package main

import (
	"os"

	"github.com/jarvanstack/mysqldump"
)

func main() {

	dns := "root:rootpasswd@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai"

	f, _ := os.Create("dump.sql")

	_ = mysqldump.Dump(
		dns,                          // DNS
		mysqldump.WithDropTable(),    // Option: Delete table before create (Default: Not delete table)
		mysqldump.WithData(),         // Option: Dump Data (Default: Only dump table schema)
		mysqldump.WithTables("test"), // Option: Dump Tables (Default: All tables)
		mysqldump.WithWriter(f),      // Option: Writer (Default: os.Stdout)
		mysqldump.WithDBs("dc3"),     // Option: Dump Dbs (Default: db in dns)
	)
}
