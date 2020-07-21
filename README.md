# [sqltocsvgzip](https://pkg.go.dev/github.com/thatInfrastructureGuy/sqltocsvgzip) [![Build Status](https://travis-ci.com/thatInfrastructureGuy/sqltocsvgzip.svg?branch=master)](https://travis-ci.com/github/thatInfrastructureGuy/sqltocsvgzip)

A library designed to convert sql.Rows result from a query into a **CSV.GZIP** file and/or **upload to AWS S3**.

## Features
* `UploadToS3(rows)`:  `sql -> csv -> gzip -> S3`
* `WriteFile(rows, filename)`: `sql -> csv -> gzip -> file`
* Multi-threaded Gzip compression
* Concurrent multipart S3 uploads
* Upload retries for resiliency
* Uploading to S3 does not require local storage.
* Consistent memory, cpu and network usage whether your database has 1 Million or 1 Trillion records.
 
### Installation
```go 
go get github.com/thatInfrastructureGuy/sqltocsvgzip@v0.0.6
```

_Note: Please do not use master branch. Master branch may contain breaking changes. Use tags instead._

### Usage

_Note: Check out [examples](https://github.com/thatInfrastructureGuy/sqltocsvgzip/tree/master/examples) directory for examples._

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

config := sqltocsvgzip.UploadConfig(rows)
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

csvConverter := sqltocsvgzip.WriteConfig(rows)

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

### Defaults
* 4096 rows of default sql batch.
* 50Mb default csv buffer size.
* 50Mb default pgzip buffer size.
* Zipping: Default runtime.GOMAXPROCS(0) goroutines with 1Mb data/goroutine
* Uploading: Default runtime.GOMAXPROCS(0) goroutines.

### Caveats
* Minimum PartUploadSize should be greater than 5 Mb.
* Maximum of 10000 part uploads are allowed by AWS. Hence, (50Mb x 10000) `500Gb` of gzipped data is supported by default settings.
* Increase buffer size if you want to reduce parts or have more than 500Gb of gzipped data.
* Currently only supports upload to AWS S3 API compatible storage.

### System Requirements
* Minimum:
    * CPU: 2 vcpu
    * Memory: [(vcpu + 3) x PartUploadSize](https://github.com/thatInfrastructureGuy/sqltocsvgzip/wiki/Memory-Footprint-Calculation)
    * Disk: Only needed if your writing to a file locally. (> size of gzip file)

---

Credits:
* [joho/sqltocsv](github.com/joho/sqltocsv)
* [klauspost/pgzip](https://github.com/klauspost/pgzip)
