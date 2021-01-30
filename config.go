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
	CsvBufferSize         int
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
	RowCount              int64

	s3Svc            *s3.S3
	s3Resp           *s3.CreateMultipartUploadOutput
	s3CompletedParts []*s3.CompletedPart
	rows             *sql.Rows
	rowPreProcessor  CsvPreProcessorFunc
	gzipBuf          []byte
	partNumber       int64
	uploadQ          chan *obj
	quit             chan error
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

func getLogLevel() (level LogLevel) {
	levels := map[string]LogLevel{
		"ERROR":   Error,
		"WARN":    Warn,
		"INFO":    Info,
		"DEBUG":   Debug,
		"VERBOSE": Verbose,
	}

	var ok bool
	if level, ok = levels[os.Getenv("LOG_LEVEL")]; !ok {
		level = Info
	}

	return
}

// WriteConfig will return a Converter which will write your CSV however you like
// but will allow you to set a bunch of non-default behaivour like overriding
// headers or injecting a pre-processing step into your conversion
func WriteConfig(rows *sql.Rows) *Converter {
	return &Converter{
		rows:                  rows,
		quit:                  make(chan error, 1),
		WriteHeaders:          true,
		Delimiter:             ',',
		CsvBufferSize:         10 * 1024 * 1024,
		CompressionLevel:      flate.DefaultCompression,
		GzipGoroutines:        runtime.GOMAXPROCS(0), // Should be atleast the number of cores. Not sure how it impacts cgroup limits.
		GzipBatchPerGoroutine: 512 * 1024,            // Should be atleast 100K
		LogLevel:              getLogLevel(),
	}
}

// UploadConfig sets the default values for Converter struct.
func UploadConfig(rows *sql.Rows) *Converter {
	return &Converter{
		rows:                  rows,
		quit:                  make(chan error, 1),
		WriteHeaders:          true,
		Delimiter:             ',',
		CompressionLevel:      flate.DefaultCompression,
		CsvBufferSize:         10 * 1024 * 1024,
		GzipGoroutines:        runtime.GOMAXPROCS(0), // Should be atleast the number of cores. Not sure how it impacts cgroup limits.
		GzipBatchPerGoroutine: 512 * 1024,            // Should be atleast 100K
		LogLevel:              getLogLevel(),
		S3Upload:              true,
		UploadThreads:         4,
		UploadPartSize:        50 * 1024 * 1025, // Should be greater than 5 * 1024 * 1024 for s3 upload
		S3Bucket:              os.Getenv("S3_BUCKET"),
		S3Path:                os.Getenv("S3_PATH"),
		S3Region:              os.Getenv("S3_REGION"),
		S3Acl:                 os.Getenv("S3_ACL"),
	}
}
