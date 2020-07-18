####  Set up

Multiple ways to set the library:

1. Environment Variables  the library looks for

```
GzipGoroutines:        runtime.GOMAXPROCS(0),  # Dont change normally
UploadThreads:         runtime.GOMAXPROCS(0),  # Dont change normally
S3Bucket:              os.Getenv("S3_BUCKET"), # Required
S3Path:                os.Getenv("S3_PATH"),   # Required
S3Region:              os.Getenv("S3_REGION"), # Required
S3Acl:                 os.Getenv("S3_ACL"),    # Optional
```

2. Change the config when getting the default config to WriteFile.

```
cfg := sqltocsvgzip.WriteConfig() // To write a file
cfg.SqlBatchSize = 1000
```

3. Change the config when getting the default config to UploadToS3.

```
cfg := sqltocsvgzip.UploadConfig() // To upload to S3
cfg.S3Path = "test.csv.gz"
cfg.S3Region = "us-east-1"
cfg.S3Bucket - "mybucket"
```
