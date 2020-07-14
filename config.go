package sqltocsvgzip

import (
	"compress/flate"
	"database/sql"
	"os"

	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	minFileSize = 5 * 1024 * 1024
)

type obj struct {
	partNumber int64
	buf        []byte
}

type LogLevel int

const (
	Debug LogLevel = 4
	Info  LogLevel = 3
	Warn  LogLevel = 2
	Error LogLevel = 1
)

// Converter does the actual work of converting the rows to CSV.
// There are a few settings you can override if you want to do
// some fancy stuff to your CSV.
type Converter struct {
	LogLevel              LogLevel
	Headers               []string // Column headers to use (default is rows.Columns())
	WriteHeaders          bool     // Flag to output headers in your CSV (default is true)
	TimeFormat            string   // Format string for any time.Time values (default is time's default)
	Delimiter             rune     // Delimiter to use in your CSV (default is comma)
	SqlBatchSize          int
	CompressionLevel      int
	GzipGoroutines        int
	GzipBatchPerGoroutine int
	SingleThreaded        bool
	S3Bucket              string
	S3Region              string
	S3Acl                 string
	S3Path                string
	S3Upload              bool
	UploadThreads         int
	UploadPartSize        int64

	s3Svc            *s3.S3
	s3Resp           *s3.CreateMultipartUploadOutput
	s3CompletedParts []*s3.CompletedPart
	rows             *sql.Rows
	rowPreProcessor  CsvPreProcessorFunc
	uploadQ          chan *obj
	quit             chan bool
}

// CsvPreprocessorFunc is a function type for preprocessing your CSV.
// It takes the columns after they've been munged into strings but
// before they've been passed into the CSV writer.
//
// Return an outputRow of false if you want the row skipped otherwise
// return the processed Row slice as you want it written to the CSV.
type CsvPreProcessorFunc func(row []string, columnNames []string) (outputRow bool, processedRow []string)

// SetRowPreProcessor lets you specify a CsvPreprocessorFunc for this conversion
func (c *Converter) SetRowPreProcessor(processor CsvPreProcessorFunc) {
	c.rowPreProcessor = processor
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
		GzipGoroutines:        10,
		GzipBatchPerGoroutine: 100000,
		LogLevel:              Info,
	}
}

// DefaultConfig sets the default values for Converter struct.
func DefaultConfig(rows *sql.Rows) *Converter {
	return &Converter{
		rows:                  rows,
		WriteHeaders:          true,
		Delimiter:             ',',
		CompressionLevel:      flate.DefaultCompression,
		GzipGoroutines:        10,
		GzipBatchPerGoroutine: 100000,
		S3Upload:              true,
		UploadThreads:         6,
		UploadPartSize:        5 * 1024 * 1025, // Should be greater than 5 * 1024 * 1024
		S3Bucket:              os.Getenv("S3_BUCKET"),
		S3Path:                os.Getenv("S3_PATH"),
		S3Region:              os.Getenv("S3_REGION"),
		S3Acl:                 os.Getenv("S3_ACL"),
		LogLevel:              Info,
	}
}
