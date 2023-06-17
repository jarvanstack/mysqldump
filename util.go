package mysqldump

import (
	"fmt"
	"strings"
)

//从dns中提取出数据库名称，并将其作为结果返回。
//如果无法解析出数据库名称，将返回一个错误。

func GetDBNameFromDNS(dns string) (string, error) {
	ss1 := strings.Split(dns, "/")
	if len(ss1) == 2 {
		ss2 := strings.Split(ss1[1], "?")
		if len(ss2) == 2 {
			return ss2[0], nil
		}
	}

	return "", fmt.Errorf("dns error: %s", dns)
}
