package collect

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"pgstats/utils"
)

var failed_metric []string
var startLsn string
var engineMajorVersion string
var most_frequent_job_status int

func connect(host string, port int, user string, password string, dbname string) *sql.DB {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	utils.LogFatal("Could not create database connection", err)

	err = db.Ping()
	utils.LogFatal("Could not ping the database", err)

	_, err = db.Exec("set statement_timeout = 30000")
	utils.LogFatal("Could not configure connection statement_timeout", err)

	_, err = db.Exec("set idle_in_transaction_session_timeout = 30000")
	utils.LogFatal("Could not configure connection idle_in_transaction_session_timeout", err)

	_, err = db.Exec("set transaction read only")
	utils.LogFatal("Could not configure connection read only", err)

	log.Println("Successfully connected!")
	return db
}

func writeCSV(output string, db *sql.DB, q string, k string) {
	rows, err := db.Query(q)
	utils.LogError(fmt.Sprintf("Could not query metric %s", k), err)
	if err != nil {
		if !utils.Contains(failed_metric, k) {
			failed_metric = append(failed_metric, k)
		}
		return
	}

	defer rows.Close()

	columnNames, _ := rows.Columns()
	rowCount := len(columnNames)

	// create the directory
	dirpath := filepath.Join(output, k)
	err = os.MkdirAll(dirpath, os.ModePerm)
	utils.LogFatal("Cannot create directory", err)

	count := 1
	var w *csv.Writer

	for rows.Next() {
		row := utils.GetRow(rows, rowCount, columnNames)

		if count == 1 {
			// create the full file path and the actual file handle
			fileName := fmt.Sprintf("%s", row[0]) + ".csv"
			fullfilename := filepath.Join(dirpath, fileName)
			file, err := os.Create(fullfilename)
			utils.LogFatal("Cannot create file", err)
			// defer the closing of the file
			// create the csv writer handle
			w = csv.NewWriter(file)
			defer file.Close()

			w.Write(columnNames)
			count++
		}

		w.Write(row)
		w.Flush()

	}
}

func runEvery(o string, db *sql.DB, q map[string]string, i int, s int, host string, dbname string, th int) {
	//os.RemoveAll(o)
	for ii := 1; ii <= i; ii++ {
		for k, q := range q {
			if k == "pg_stat_activity" {
				qs := fmt.Sprintf(q, snapshotTime, startLsn)
				writeCSV(o, db, qs, k)
			} else if k == "pg_version" {
				qs := fmt.Sprintf(q, snapshotTime, host, dbname)
				writeCSV(o, db, qs , k)
			} else {
				qs := fmt.Sprintf(q, snapshotTime)
				writeCSV(o, db, qs , k)
			}
		}
		time.Sleep(time.Duration(s) * time.Second)

		if math.Mod(float64(ii), 30) == 0 || ii == 1 {
			log.Println("thread = " + strconv.Itoa(th) + " completed " + strconv.Itoa(ii) + " samples")
		}

		if most_frequent_job_status == 1 {
			break
		}

	}
	log.Println("Done Capturing Samples for thread " + strconv.Itoa(th))
}

// exported function
func Run(host string, port int, user string, password string, dbname string, timeout int, output string, gather_hammer bool) {
	var statsMap map[string]string
	var wg sync.WaitGroup
	var db4 *sql.DB

	if timeout < 60 {
		log.Fatal("timeout cannot be less than 10 seconds")
	}

	activity_to_stats_ratio := .05

	activity_it := timeout
	activity_sleep := 1
 
	stats_it := int(float64(timeout) * activity_to_stats_ratio)
	stats_sleep := int(timeout / stats_it)

	statsinfo_it := int(float64(timeout)/30)
	statsinfo_sleep := 30

	log.Println("Running stats a total of", stats_it , "times. Sampling every", stats_sleep, "seconds")
	log.Println("Running activity a total of", activity_it, "times. Sampling every", activity_sleep, "seconds")
	log.Println("Running stats metadata a total of", statsinfo_it, "times. Sampling every", statsinfo_sleep, "seconds")

	db := connect(host, port, user, password, dbname)
	defer db.Close()

	db2 := connect(host, port, user, password, dbname)
	defer db.Close()

	db3 := connect(host, port, user, password, dbname)
	defer db3.Close()

	if gather_hammer {
		db4 = connect(host, port, user, password, dbname)
		defer db4.Close()
	}

	startLsn = utils.GetQuery(db, lsnStartQuery, "lsn_start").Data[0][0]
	engineMajorVersion, err := strconv.Atoi(utils.GetQuery(db,  getEngineMajorVersion, "engine_major_version").Data[0][0])
	utils.LogFatal("Could not retrieve major engine version", err)

	if engineMajorVersion <=9 {
		utils.LogFatal("Can't start collection process", fmt.Errorf("Only Postgres 10+ supported"))
	}

	if engineMajorVersion >= 13 {
		statsMap = stats13
	} else {
		statsMap = stats12
	}

	most_frequent_job_status = 0
	wg.Add(1)
	go func(o string, d *sql.DB, q map[string]string, i int, s int) {
		log.Println("Starting Activity in thread = 1. " + strconv.Itoa(i) + " samples. Sleeping for " + strconv.Itoa(s) + " seconds between samples.")
		runEvery(o, d, q, i, s, host, dbname, 1)
		log.Println("Activity Collection Done. Thread = 1")
		wg.Done()
		most_frequent_job_status = 1
	}(output, db, activity, activity_it, activity_sleep)

	wg.Add(1)
	go func(o string, d *sql.DB, q map[string]string, i int, s int) {
		log.Println("Starting Stats in thread = 2. " + strconv.Itoa(i) + " samples. Sleeping for " + strconv.Itoa(s) + " between samples.")
		runEvery(o, d, q, i, s, host, dbname, 2)
		log.Println("Stats Collection Done. Thread = 2")
		wg.Done()
	}(output, db2, statsMap, stats_it, stats_sleep)

	wg.Add(1)
	go func(o string, d *sql.DB, q map[string]string, i int, s int) {
		log.Println("Starting Snapshot Info in thread = 3. " + strconv.Itoa(i) + " samples. Sleeping for " + strconv.Itoa(s) + " between samples.")
		runEvery(o, d, q, i, s, host, dbname, 3)
		log.Println("Snapshot Info Collection. Thread = 3")
		wg.Done()
	}(output, db3, statsInfo, statsinfo_it, statsinfo_sleep)

	if gather_hammer {
		// hammer runs at the same frequency as stats
		wg.Add(1)
		go func(o string, d *sql.DB, q map[string]string, i int, s int) {
			log.Println("Starting Hammer in thread = 4. " + strconv.Itoa(i) + " samples. Sleeping for " + strconv.Itoa(s) + " between samples.")
			runEvery(o, d, q, i, s, host, dbname, 4)
			log.Println("Hammer Collection Done. Thread = 4")
			wg.Done()
		}(output, db4, hammer, stats_it, stats_sleep)
	}

	wg.Wait()

}
