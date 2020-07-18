package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/thatInfrastructureGuy/sqltocsvgzip"
)

func main() {

	dbQuery, ok := os.LookupEnv("DB_QUERY")
	if !ok {
		log.Fatal("Need a query")
	}

	dbConnection := getDBConnection()
	db, err := sql.Open("mysql", dbConnection)
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
// from MYSQL environment variables
func getDBConnection() string {
	user, ok := os.LookupEnv("MYSQL_USER")
	if !ok {
		user = "root"
	}
	pass, ok := os.LookupEnv("MYSQL_PWD")
	if !ok {
		pass = "password"
	}
	host, ok := os.LookupEnv("MYSQL_HOST")
	if !ok {
		host = "127.0.0.1"
	}
	port, ok := os.LookupEnv("MYSQL_PORT")
	if !ok {
		port = "3306"
	}
	db, ok := os.LookupEnv("MYSQL_DB")
	if !ok {
		db = "test"
	}

	log.Printf("Connection string: %s:<password>@tcp(%s:%s)/%s", user, host, port, db)
	return user + ":" + pass + "@tcp(" + host + ":" + port + ")/" + db
}

func setConfig(rows *sql.Rows) (*sqltocsvgzip.Converter, error) {
	// Get default configuration
	config := sqltocsvgzip.DefaultConfig(rows)

	if len(config.S3Path) != 0 {
		config.S3Path = "myfolder/test.csv.gzip"
	}

	return config, nil
}
