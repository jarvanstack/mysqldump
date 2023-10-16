package main

import (
	"os"

	"github.com/jarvanstack/mysqldump"
)

func main() {

	dns := "root:rootpasswd@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai"
	f, _ := os.Open("dump.sql")

	_ = mysqldump.Source(
		dns,
		f,
		mysqldump.WithMergeInsert(1000), // Option: Merge insert 1000 (Default: Not merge insert)
		mysqldump.WithDebug(),           // Option: Print execute sql (Default: Not print execute sql)
	)
}
