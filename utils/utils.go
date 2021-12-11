package utils

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"pgstats/common"

	"golang.org/x/crypto/ssh/terminal"
)

func FileNameWithoutExtSliceNotation(fileName string) string {
	return fileName[:len(fileName)-len(filepath.Ext(fileName))]
}

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func FilePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root+string(filepath.Separator)+".", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && path != root {
			files = append(files, filepath.Base(path))
		}
		return nil
	})
	return files, err
}

func GetPassword() string {
	fmt.Println("\nPassword: ")
	// https://godoc.org/golang.org/x/crypto/ssh/terminal#ReadPassword
	// terminal.ReadPassword accepts file descriptor as argument, returns byte slice and error.
	passwd, e := terminal.ReadPassword(int(os.Stdin.Fd()))
	if e != nil {
		log.Fatal(e)
	}
	// Type cast byte slice to string.
	return string(passwd)
}

func Contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func LogFatal(message string, err error) {
	if err != nil {
		log.Fatal(fmt.Sprintf(message+": %s", err))
	}
}

func LogError(message string, err error) {
	if err != nil {
		log.Print(fmt.Sprintf(message+": %s", err))
	}
}

func GetRow(rows *sql.Rows, columnCount int, columnNames []string) []string {
	values := make([]interface{}, columnCount)
	valuePtrs := make([]interface{}, columnCount)

	row := make([]string, columnCount)

	for i, _ := range columnNames {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		LogFatal("Could not scan row in temporary table", err)
	}

	for i, _ := range columnNames {
		rawValue := values[i]

		byteValue, ok := rawValue.([]byte)
		if ok {
			row[i] = string(byteValue)
		}

		strValue, ok := rawValue.(string)
		if ok {
			row[i] = strValue
		}

		timeValue, ok := rawValue.(time.Time)
		if ok {
			row[i] = timeValue.Format(time.RFC822)
		}

		f64Value, ok := rawValue.(float64)
		if ok {
			row[i] = fmt.Sprintf("%f", f64Value)
		}

		f32Value, ok := rawValue.(float32)
		if ok {
			row[i] = fmt.Sprintf("%f", f32Value)
		}

		intValue, ok := rawValue.(int)
		if ok {
			row[i] = fmt.Sprintf("%d", intValue)
		}

		int64Value, ok := rawValue.(int64)
		if ok {
			row[i] = fmt.Sprintf("%d", int64Value)
		}

	}

	return row
}

func GetQuery(db *sql.DB, q string, k string) common.MetricQuery {

	var ret common.MetricQuery

	rows, err := db.Query(q)
	LogError(fmt.Sprintf("Could not query metric in temporary database %s", k), err)
	if err != nil {
		return ret
	}

	defer rows.Close()

	columnNames, _ := rows.Columns()
	count := len(columnNames)

	log.Println(fmt.Sprintf("start running query for %s", k))

	for rows.Next() {
		///
		row := GetRow(rows, count, columnNames)
		////
		ret.Data = append(ret.Data, row)

	}

	ret.ColumnList = columnNames
	log.Println(fmt.Sprintf("done running query for %s", k))
	return ret
}
