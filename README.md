# sqltocsvgzip [![Build Status](https://travis-ci.org/thatinfrastructureguy/sqltocsvgzip.svg?branch=master)](https://travis-ci.org/thatinfrastructureguy/sqltocsvgzip)

A library designed to convert sql.Rows result from a query into a CSV.GZIP file and/or upload to AWS S3.

## Features
* Multi-threaded Gzip compression
* One liner:  `sql -> csv -> gzip -> S3` process
* Multipart S3 with retries for resiliency
* No writable disk required when uploading to S3.
* Consistent memory, cpu and network usage whether your database has 1 Million or 1 Trillion records.

### Defaults
* 4096 rows of default sql batch.
* ~5Mb default csv buffer size.
* ~5Mb default pgzip buffer size.
* pgzip: Default 10 goroutines with 100K data/goroutine
* UploadtoS3: Default 6 concurrent uploads.

### Caveats
* Maximum of 10000 part uploads are allowed by AWS. Hence, (5Mb x 10000) `50Gb` of gzipped data is supported by default settings.
* Increase buffer size if you want to reduce parts or have more than 50Gb of gzipped data.
* Currently only supports upload to AWS S3 API compatible storage.

### Installation
```go 
go get github.com/thatInfrastructureGuy/sqltocsvgzip@v0.0.4
```

_Note: Please do not use master branch. Master branch may contain breaking changes. Use tags instead._

### Usage

Importing the package

```go
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql" // or the driver of your choice
    "github.com/thatinfrastructureguy/sqltocsvgzip"
)
```

1. Dumping a query to a file

```go
rows, _ := db.Query("SELECT * FROM users WHERE something=72")

err := sqltocsvgzip.WriteFile("~/important_user_report.csv.gzip", rows)
if err != nil {
    panic(err)
}
```

2. Upload to AWS S3 with env vars

```go
rows, _ := db.Query("SELECT * FROM users WHERE something=72")

// UploadToS3 looks for the followinging environment variables.
// Required: S3_BUCKET, S3_PATH, S3_REGION
// Optional: S3_ACL (default => bucket-owner-full-control)
err := sqltocsvgzip.UploadToS3(rows)
if err != nil {
    panic(err)
}
```

3. Upload to AWS S3 without environment variables

```go
rows, _ := db.Query("SELECT * FROM users WHERE something=72")

config := sqltocsvgzip.DefaultConfig(rows)
config.S3Bucket = "mybucket"
config.S3Path = "/myfolder/file.csv.gzip"
config.S3Region = "us-west-1"

err := sqltocsvgzip.Upload()
if err != nil {
    panic(err)
}
```

4. Return a query as a GZIP download on the world wide web

```go
http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    rows, err := db.Query("SELECT * FROM users WHERE something=72")
    if err != nil {
        http.Error(w, err, http.StatusInternalServerError)
        return
    }
    defer rows.Close()

    w.Header().Set("Content-Type", "text/csv")
    w.Header().Set("Content-Encoding", "gzip")
    w.Header().Set("Content-Disposition", "attachment; filename=\"report.csv.gzip\"")

    sqltocsvgzip.Write(w, rows)
})
http.ListenAndServe(":8080", nil)
```

If you need more flexibility you can get an instance of a `Converter` and fiddle with a few settings.

```go
rows, _ := db.Query("SELECT * FROM users WHERE something=72")

csvConverter := sqltocsvgzip.New(rows)

csvConverter.TimeFormat = time.RFC822
csvConverter.Headers = append(rows.Columns(), "extra_column_one", "extra_column_two")

csvConverter.SetRowPreProcessor(func (columns []string) (bool, []string) {
    // exclude admins from report
    // NOTE: this is a dumb example because "where role != 'admin'" is better
    // but every now and then you need to exclude stuff because of calculations
    // that are a pain in sql and this is a contrived README
    if columns[3] == "admin" {
      return false, []string{}
    }

    extra_column_one = generateSomethingHypotheticalFromColumn(columns[2])
    extra_column_two = lookupSomeApiThingForColumn(columns[4])

    return append(columns, extra_column_one, extra_column_two)
})

csvConverter.WriteFile("~/important_user_report.csv.gzip")
```

---

For more details on what else you can do to the `Converter` see the [sqltocsvgzip godocs](https://pkg.go.dev/github.com/thatInfrastructureGuy/sqltocsvgzip)

---

Credits to @joho for his work on [joho/sqltocsv](github.com/joho/sqltocsv)
