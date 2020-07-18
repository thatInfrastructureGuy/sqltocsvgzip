package main

import (
	"database/sql"
	"log"
	"net/url"
	"os"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/thatInfrastructureGuy/sqltocsvgzip"
)

func main() {

	dbQuery, ok := os.LookupEnv("DB_QUERY")
	if !ok {
		log.Fatal("Need a query")
	}

	dbConnection := getDBConnection()
	db, err := sql.Open("mssql", dbConnection)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(dbQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	sqlGzip, err := setConfig(rows)
	if err != nil {
		log.Fatal(err)
	}

	err = sqlGzip.Upload()
	if err != nil {
		log.Fatal(err)
	}
}

// getDBConnection generates database connection string
// from Postgres environment variables
func getDBConnection() string {
	user, ok := os.LookupEnv("DB_USER")
	if !ok {
		user = "root"
	}
	pass, ok := os.LookupEnv("DB_PWD")
	if !ok {
		pass = "password"
	}
	host, ok := os.LookupEnv("DB_HOST")
	if !ok {
		host = "127.0.0.1"
	}
	port, ok := os.LookupEnv("DB_PORT")
	if !ok {
		port = "1433"
	}
	db, ok := os.LookupEnv("DB_DB")
	if !ok {
		db = "test"
	}

	log.Printf("Connection string: sqlserver://%s:<password>@%s:%s?database=%s", user, host, port, db)
	return "sqlserver://" + user + ":" + url.QueryEscape(pass) + "@" + host + ":" + port + "?database=" + db
}

func setConfig(rows *sql.Rows) (*sqltocsvgzip.Converter, error) {
	// Get default configuration
	config := sqltocsvgzip.UploadConfig(rows)

	if len(config.S3Path) != 0 {
		config.S3Path = "myfolder/test.csv.gzip"
	}

	return config, nil
}
