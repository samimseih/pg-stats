package report

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"pgstats/common"
	"pgstats/utils"

	"github.com/mattn/go-sqlite3"
)

/// sqlite custom functions
///// Computes the standard deviation of a GROUPed BY set of values
type stddev struct {
	xs []int64
	// Running average calculation
	sum int64
	n   int64
}

func newStddev() *stddev { return &stddev{} }

func (s *stddev) Step(x int64) {
	s.xs = append(s.xs, x)
	s.sum += x
	s.n++
}

func (s *stddev) Done() float64 {
	mean := float64(s.sum) / float64(s.n)
	var sqDiff []float64
	for _, x := range s.xs {
		sqDiff = append(sqDiff, math.Pow(float64(x)-mean, 2))
	}
	var dev float64
	for _, x := range sqDiff {
		dev += x
	}
	dev /= float64(len(sqDiff))
	return math.Sqrt(dev)
}

func getMetricData(md *common.MetricData, filesToProcess []string, metric string) {

	i := 0
	for _, v := range filesToProcess {
		vv, err := os.Open(v)
		utils.LogFatal("Couldn't open the csv file", err)
		r := csv.NewReader(vv)

		// Iterate through the records
		ii := 0
		for {
			record, err := r.Read()

			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}

			if ii == 0 {
				if i == 0 {
					noColumns := len(record)
					columnList := record
					md.ColumnList = columnList
					md.ColumnLength = noColumns
					i++
				}
				ii++
			} else {
				md.Data = append(md.Data, record)
			}
		}
		vv.Close()
	}
}

func importMetric(db *sql.DB, metric string, output string, start int64, end int64) error {

	var err error

	md := common.MetricData{}

	filesToProcess := getFilesToProcess(output, metric, start, end)
	getMetricData(&md, filesToProcess, metric)

	if md.ColumnLength == 0 {
		return errors.New("Empty MetricData")
	}

	ddl := "create table " + metric + " (" + strings.Join(md.ColumnList[:], " TEXT,") + " string)"
	_, err = db.Exec(ddl)
	utils.LogFatal("Could not create temporary table ", err)

	dml := "insert into " + metric + " (" + strings.Join(md.ColumnList[:], ",") + ") values (" + strings.Repeat("?,", md.ColumnLength)[:md.ColumnLength*2-1] + ")"
	log.Println(fmt.Sprintf("start loading %s", metric))

	for _, v := range md.Data {
		s := make([]interface{}, len(v))
		for i, v := range v {
			s[i] = v
		}
		_, err = db.Exec(dml, s...)
		utils.LogFatal("Could not insert row", err)
	}
	log.Println(fmt.Sprintf("done loading %s", metric))

	return err

}

func dropMetric(db *sql.DB, metric string) {
	_, err := db.Exec(fmt.Sprintf("drop table %s", metric))
	utils.LogError("Could not drop temporary table ", err)
	log.Printf(fmt.Sprintf("dropped table %s", metric))
}

func filesToProcessRemove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func getFilesToProcess(dir string, metric string, start int64, end int64) []string {
	var filesToProcess []string

	dirpath := filepath.Join(dir, metric)
	filesReturned, err := filepath.Glob(fmt.Sprintf("%s/*", dirpath))
	utils.LogFatal("Could not find metric ", err)

	for _, f := range filesReturned {
		if start > 0 && end > 0 {
			n, err := strconv.ParseInt(utils.FileNameWithoutExtSliceNotation(path.Base(f)), 10, 64)
			utils.LogFatal("Snapshot file not valid", err)
			if n >= start && n <= end {
				filesToProcess = append(filesToProcess, f)
			}
		} else {
			filesToProcess = append(filesToProcess, f)
		}
	}

	return filesToProcess
}

func renderHTML(v common.MetricQuery, class string, caption string) string {
	var sb strings.Builder

	if class != "" {
		sb.WriteString(fmt.Sprintf(`<table class="%s">`, class))
	} else {
		sb.WriteString(`<table>`)
	}

	if caption != "" {
		sb.WriteString(fmt.Sprintf(`<caption>%s</caption>`, caption))
	}
	sb.WriteString(`<tr>`)
	for _, vv := range v.ColumnList {
		sb.WriteString(`<th>`)
		sb.WriteString(vv)
		sb.WriteString(`</th>`)
	}
	sb.WriteString("</tr>")

	for _, vv := range v.Data {
		sb.WriteString(`<tr>`)
		for _, vvv := range vv {
			sb.WriteString(`<td>`)
			sb.WriteString(vvv)
			sb.WriteString(`</td>`)
		}
		sb.WriteString(`</tr>`)
	}

	sb.WriteString(`</table>`)
	return sb.String()
}

func getSqlite() *sql.DB {
	sql.Register("sqlite3_custom", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := conn.RegisterAggregator("stddev", newStddev, true); err != nil {
				return err
			}
			return nil
		},
	})
	db, err := sql.Open("sqlite3_custom", "file::memory:?cache=shared")
	utils.LogFatal("Could not create database connection", err)
	return db
}

// exported function
func Run(output string, fileout string, start int64, end int64) {

	var host string
	var dbname string
	var engine_version string
	var major_version int

	var queryOut common.MetricQuery
	var render bool
	var html_class string
	var header string
	var query string
	var metric string
	var title string

	db := getSqlite()
	defer db.Close()

	var sb strings.Builder

	no_sections := len(sections)
	sections_found, err := utils.FilePathWalkDir(output)

	if len(sections_found) == 0 {
		utils.LogFatal("Can't generate a report", fmt.Errorf("No snapshot data found in %s", output))
	}

	if utils.StringInSlice("pg_version", sections_found) {
		_ = importMetric(db, "pg_version", output, start, end)
		version_info := utils.GetQuery(db, "select distinct host, dbname, engine_version, substr(engine_version, instr(engine_version,' ')+1, 2) major_version from pg_version", "pg_version")
		host = version_info.Data[0][0]
		dbname = version_info.Data[0][1]
		engine_version = version_info.Data[0][2]
		major_version, err = strconv.Atoi(version_info.Data[0][3])
		utils.LogFatal("Could not retreive major version from snapshots", err)
	}

	for i := 0; i < no_sections; i++ {
		for i, v := range sections[i] {
			metric = i
			if !utils.StringInSlice(i, sections_found) {
				for ii, _ := range v {
					header = ii
					html = strings.Replace(html, "{"+header+"}", "", 1)
				}
			} else {
				err = importMetric(db, metric, output, start, end)

				for ii, vv := range v {
					header = ii
					for iii, vvv := range vv {
						if iii == "html_class" {
							html_class = vvv
						}
						if iii == "query" {
							query = vvv
						}
						if iii == "title" {
							title = vvv
						}
						if iii == "render" {
							if vvv == "f" || vvv == "" {
								render = false
							} else {
								render = true
							}
						}
						if iii == "minMajorVersion" {
							vvvInt, err := strconv.Atoi(vvv)
							utils.LogFatal("Invalid minMajorVersion value detected", err)
							if major_version < vvvInt {
								render = false
								goto renderhtml
							}
						}
					}

					query = strings.Replace(query, "{metric}", metric, 5)
					query = strings.Replace(query, "{host}", host, 5)
					query = strings.Replace(query, "{engine_version}", engine_version, 5)
					query = strings.Replace(query, "{dbname}", dbname, 5)
					query = strings.Replace(query, "{start}", strconv.FormatInt(start, 10), 5)
					query = strings.Replace(query, "{end}", strconv.FormatInt(end, 10), 5)
					queryOut = utils.GetQuery(db, query, metric)

					renderhtml:
					if render {
						html = strings.Replace(html, "{"+header+"}", renderHTML(queryOut, html_class, title), 1)
					} else {
						html = strings.Replace(html, "{style_"+header+"}", "display:none;", 1)
					}
					render = true
					
				}
				dropMetric(db, metric)
			}
		}
	}
	sb.WriteString(html)

	f, err := os.Create(fileout)
	utils.LogFatal("Could not create output file", err)
	defer f.Close()
	_, err = f.WriteString(sb.String())
	utils.LogFatal("Could not write output file", err)

}
