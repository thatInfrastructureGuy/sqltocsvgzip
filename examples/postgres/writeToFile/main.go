package main

import (
	"database/sql"
	"log"
	"net/url"
	"os"

	_ "github.com/lib/pq"
	"github.com/thatInfrastructureGuy/sqltocsvgzip"
)

func main() {

	dbQuery, ok := os.LookupEnv("DB_QUERY")
	if !ok {
		log.Fatal("Need a query")
	}

	dbConnection := getDBConnection()
	db, err := sql.Open("postgres", dbConnection)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(dbQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	err = sqltocsvgzip.WriteFile("test.csv.gzip", rows)
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
		port = "5432"
	}
	db, ok := os.LookupEnv("DB_DB")
	if !ok {
		db = "test"
	}

	log.Printf("Connection string: postgres://%s:<password>@%s:%s/%s", user, host, port, db)
	return "postgres://" + user + ":" + url.QueryEscape(pass) + "@" + host + ":" + port + "/" + db
}
