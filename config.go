package sqltocsvgzip

import (
	"compress/flate"
	"database/sql"
	"os"
	"runtime"

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
	Error   LogLevel = 1
	Warn    LogLevel = 2
	Info    LogLevel = 3
	Debug   LogLevel = 4
	Verbose LogLevel = 5
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
	S3Bucket              string
	S3Region              string
	S3Acl                 string
	S3Path                string
	S3Upload              bool
	UploadThreads         int
	UploadPartSize        int

	s3Svc            *s3.S3
	s3Resp           *s3.CreateMultipartUploadOutput
	s3CompletedParts []*s3.CompletedPart
	rows             *sql.Rows
	rowPreProcessor  CsvPreProcessorFunc
	gzipBuf          []byte
	partNumber       int64
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
		GzipGoroutines:        runtime.GOMAXPROCS(0),
		GzipBatchPerGoroutine: 1 * 1024 * 1024,
		UploadPartSize:        5 * 1024 * 1025, // Should be greater than 1 * 1024 * 1024 for pgzip
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
		GzipGoroutines:        runtime.GOMAXPROCS(0),
		GzipBatchPerGoroutine: 1 * 1024 * 1024,
		S3Upload:              true,
		UploadThreads:         runtime.GOMAXPROCS(0),
		UploadPartSize:        50 * 1024 * 1025, // Should be greater than 5 * 1024 * 1024 for s3 upload
		S3Bucket:              os.Getenv("S3_BUCKET"),
		S3Path:                os.Getenv("S3_PATH"),
		S3Region:              os.Getenv("S3_REGION"),
		S3Acl:                 os.Getenv("S3_ACL"),
		LogLevel:              Info,
	}
}
