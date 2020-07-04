package sqltocsvgzip

import (
	"compress/flate"
	"database/sql"

	"github.com/aws/aws-sdk-go/service/s3"
)

// Converter does the actual work of converting the rows to CSV.
// There are a few settings you can override if you want to do
// some fancy stuff to your CSV.
type Converter struct {
	Headers               []string // Column headers to use (default is rows.Columns())
	WriteHeaders          bool     // Flag to output headers in your CSV (default is true)
	TimeFormat            string   // Format string for any time.Time values (default is time's default)
	Delimiter             rune     // Delimiter to use in your CSV (default is comma)
	SqlBatchSize          int
	CompressionLevel      int
	GzipGoroutines        int
	GzipBatchPerGoroutine int
	SingleThreaded        bool
	S3Svc                 *s3.S3
	S3Resp                *s3.CreateMultipartUploadOutput
	S3Bucket              string
	S3Region              string
	S3Acl                 string
	S3Path                string
	S3Upload              bool
	S3UploadThreads       int
	S3UploadMaxPartSize   int64
	S3CompletedParts      []*s3.CompletedPart

	rows            *sql.Rows
	rowPreProcessor CsvPreProcessorFunc
}

// New will return a Converter which will write your CSV however you like
// but will allow you to set a bunch of non-default behaivour like overriding
// headers or injecting a pre-processing step into your conversion
func New(rows *sql.Rows) *Converter {
	return &Converter{
		rows:                  rows,
		WriteHeaders:          true,
		Delimiter:             ',',
		CompressionLevel:      flate.DefaultCompression,
		GzipGoroutines:        6,
		GzipBatchPerGoroutine: 180000,
	}
}

func DefaultS3Config(rows *sql.Rows) *Converter {
	return &Converter{
		rows:                  rows,
		WriteHeaders:          true,
		Delimiter:             ',',
		CompressionLevel:      flate.DefaultCompression,
		GzipGoroutines:        6,
		GzipBatchPerGoroutine: 180000,
		S3Upload:              true,
		S3UploadMaxPartSize:   50 * 1024 * 1024,
	}
}
