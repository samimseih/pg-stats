package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"pgstats/collect"
	"pgstats/report"
	"pgstats/utils"
)

func main() {

	var hostIn string
	var dbnameIn string
	var usernameIn string
	var outputIn string
	var fileOutIn string
	var portIn int
	var timeoutIn int
	var noPasswordIn bool
	var passwordIn bool
	var actionIn string
	var startIn string
	var endIn string
	var hammerIn bool

	var startEp int64
	var endEp int64

	var password string

	flag.Usage = func() {
		fmt.Fprintf(os.Stdout, "pg_stats usage menu\n")
		fmt.Fprintf(os.Stdout, "-------------------\n")

		fmt.Fprintf(os.Stdout, "-h, --host=HOSTNAME 	 database server host or socket directory\n")
		fmt.Fprintf(os.Stdout, "-d, --dbname=DBNAME      database name to connect to\n")
		fmt.Fprintf(os.Stdout, "-p, --port=portIn	 database server port ( default 5432 )\n")
		fmt.Fprintf(os.Stdout, "-U, --username=USERNAME  database user name\n")
		fmt.Fprintf(os.Stdout, "-w, --no-password        never prompt for password\n")
		fmt.Fprintf(os.Stdout, "-W, --password           force password prompt (should happen automatically)\n")
		fmt.Fprintf(os.Stdout, "-o, --output=OUTPUT      output directory of the samples ( default /tmp )\n")
		fmt.Fprintf(os.Stdout, "-t, --timeout=TIMEOUT    sampling timeout ( default 60 )\n")
		fmt.Fprintf(os.Stdout, "-a, --action=ACTION      action to perform: all, report, collect ( default 'all' ) \n")
		fmt.Fprintf(os.Stdout, "-f, --file=OUTPUT_FILE   file path ( default /tmp/out.html )\n")
		fmt.Fprintf(os.Stdout, "-s, --start=START        time of first snapshot to report \n")
		fmt.Fprintf(os.Stdout, "-e, --end=END            time of last snapshot to report\n")
		fmt.Fprintf(os.Stdout, "--hammer		 For use with HammerDB. To gather HammerDB order stats\n")

	}
	flag.StringVar(&outputIn, "output", "", "samples output location")
	flag.StringVar(&outputIn, "o", "", "samples output location")
	flag.StringVar(&fileOutIn, "file", "", "file output")
	flag.StringVar(&fileOutIn, "f", "", "file output")
	flag.StringVar(&hostIn, "host", "", "instance host/endpoint name")
	flag.StringVar(&hostIn, "h", "", "instance host/endpoint name")
	flag.StringVar(&dbnameIn, "dbname", "", "database name")
	flag.StringVar(&dbnameIn, "d", "", "database name")
	flag.StringVar(&usernameIn, "username", "", "database username")
	flag.StringVar(&usernameIn, "U", "", "database username")
	flag.StringVar(&actionIn, "action", "", "action to perform")
	flag.StringVar(&actionIn, "a", "", "action to perform")
	flag.IntVar(&portIn, "port", 5432, "instance port")
	flag.IntVar(&portIn, "p", 5432, "instance port")
	flag.IntVar(&timeoutIn, "timeout", 60, "sampling timeout")
	flag.IntVar(&timeoutIn, "t", 60, "sampling timeout")
	flag.BoolVar(&noPasswordIn, "no-password", false, "no password")
	flag.BoolVar(&noPasswordIn, "w", false, "no password")
	flag.BoolVar(&passwordIn, "password", false, "force password")
	flag.BoolVar(&passwordIn, "W", false, "force password")
	flag.StringVar(&startIn, "start", "0", "start time")
	flag.StringVar(&startIn, "s", "0", "start time")
	flag.StringVar(&endIn, "end", "0", "end time")
	flag.StringVar(&endIn, "e", "0", "end time")
	flag.BoolVar(&hammerIn, "hammer", false, "no hammerdb stats")
	flag.Parse()
	action := fmt.Sprintf(actionIn)
	host := fmt.Sprint(hostIn)
	dbname := fmt.Sprint(dbnameIn)
	username := fmt.Sprint(usernameIn)
	output := fmt.Sprint(outputIn)
	fileout := fmt.Sprint(fileOutIn)
	start := fmt.Sprint(startIn)
	end := fmt.Sprint(endIn)
	port := portIn
	timeout := timeoutIn

	if action == "" {
		action = os.Getenv("ACTION")
		if action == "" {
			action = "all"
		}
	}

	if host == "" && (action == "all" || action == "collect") {
		host = os.Getenv("PGHOST")
		if host == "" {
			log.Fatal("Host not provided")
		}
	}

	if dbname == "" && (action == "all" || action == "collect") {
		dbname = os.Getenv("PGDATABASE")
		if dbname == "" {
			log.Fatal("Database not provided")
		}
	}

	if username == "" && (action == "all" || action == "collect") {
		username = os.Getenv("PGUSER")
		if username == "" {
			log.Fatal("Username not provided")
		}
	}

	if output == "" {
		output = fmt.Sprintf("/tmp")
	}
	log.Println("Snapshot Output Directory:", output)

	if fileout == "" && (action == "all" || action == "report") {
		fileout = fmt.Sprintf("/tmp/out.html")
	}

	if (strings.Compare(start, "0") != 0 && strings.Compare(end, "0") == 0) ||
		(strings.Compare(end, "0") != 0 && strings.Compare(start, "0") == 0) {
		utils.LogFatal("start and end must be specified together", fmt.Errorf("parsing error"))
	}

	loc, _ := time.LoadLocation("UTC")

	if strings.Compare(start, "0") != 0 && ( action == "all" || action == "collect" ) {
		log.Fatal("Can't specify --start/s with action = 'collect' or 'all'")
	}

	if strings.Compare(end, "0") != 0 && ( action == "all" || action == "collect" ) {
		log.Fatal("Can't specify --end/e with action = 'collect' or 'all'")
	}

	if strings.Compare(start, "0") != 0 && (action == "all" || action == "report") {
		start, err := time.Parse("2006-01-02 15:04:00", startIn)
		utils.LogFatal("date format not valid", err)
		startEp = start.In(loc).Unix()
	} else {
		startEp = 0
	}

	if strings.Compare(end, "0") != 0 && (action == "all" || action == "report") {
		end, err := time.Parse("2006-01-02 15:04:00", endIn)
		utils.LogFatal("date format not valid", err)
		endEp = end.In(loc).Unix()
	} else {
		endEp = 0
	}

	if (endEp - startEp < 60 ) && ( endEp - startEp > 0) {
		log.Fatal("The reporting window must be at least 60 seconds")
	}

	if action == "all" || action == "report" {
		log.Println("Report Output File:", fileout)
	}

	for len(password) == 0 && (action == "all" || action == "collect") {
		password = os.Getenv("PGPASSWORD")
		if noPasswordIn {
			break
		} else if passwordIn {
			password = utils.GetPassword()
		} else {
			if password == "" {
				password = utils.GetPassword()
			}
		}
	}

	if action == "all" || action == "collect" {
		log.Println("Endpoint:", host, "Database:", dbname, "Port:", port, "Timeout:", timeout)
		collect.Run(host, port, username, password, dbname, timeout, output, hammerIn)
	}

	if action == "all" || action == "report" {
		report.Run(output, fileout, startEp, endEp)
	}
}
