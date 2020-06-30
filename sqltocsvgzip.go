// sqltocsv is a package to make it dead easy to turn arbitrary database query
// results (in the form of database/sql Rows) into CSV output.
//
// Source and README at https://github.com/joho/sqltocsv
package sqltocsvgzip

import (
	"bytes"
	"compress/flate"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/pgzip"
)

// WriteFile will write a CSV file to the file name specified (with headers)
// based on whatever is in the sql.Rows you pass in. It calls WriteCsvToWriter under
// the hood.
func WriteFile(csvGzipFileName string, rows *sql.Rows) error {
	return New(rows).WriteFile(csvGzipFileName)
}

// WriteString will return a string of the CSV. Don't use this unless you've
// got a small data set or a lot of memory
func WriteString(rows *sql.Rows) (string, error) {
	return New(rows).WriteString()
}

// Write will write a CSV file to the writer passed in (with headers)
// based on whatever is in the sql.Rows you pass in.
func Write(writer io.Writer, rows *sql.Rows) error {
	return New(rows).Write(writer)
}

// CsvPreprocessorFunc is a function type for preprocessing your CSV.
// It takes the columns after they've been munged into strings but
// before they've been passed into the CSV writer.
//
// Return an outputRow of false if you want the row skipped otherwise
// return the processed Row slice as you want it written to the CSV.
type CsvPreProcessorFunc func(row []string, columnNames []string) (outputRow bool, processedRow []string)

// Converter does the actual work of converting the rows to CSV.
// There are a few settings you can override if you want to do
// some fancy stuff to your CSV.
type Converter struct {
	Headers          []string // Column headers to use (default is rows.Columns())
	WriteHeaders     bool     // Flag to output headers in your CSV (default is true)
	TimeFormat       string   // Format string for any time.Time values (default is time's default)
	Delimiter        rune     // Delimiter to use in your CSV (default is comma)
	SqlBatchSize     int
	CompressionLevel int
	SingleThreaded   bool

	rows            *sql.Rows
	rowPreProcessor CsvPreProcessorFunc
}

// SetRowPreProcessor lets you specify a CsvPreprocessorFunc for this conversion
func (c *Converter) SetRowPreProcessor(processor CsvPreProcessorFunc) {
	c.rowPreProcessor = processor
}

// String returns the CSV as a string in an fmt package friendly way
func (c *Converter) String() string {
	csv, err := c.WriteString()
	if err != nil {
		return ""
	}
	return csv
}

// WriteString returns the CSV as a string and an error if something goes wrong
func (c *Converter) WriteString() (string, error) {
	buffer := bytes.Buffer{}
	err := c.Write(&buffer)
	return buffer.String(), err
}

// WriteFile writes the csv.gzip to the filename specified, return an error if problem
func (c *Converter) WriteFile(csvGzipFileName string) error {
	f, err := os.Create(csvGzipFileName)
	if err != nil {
		return err
	}

	err = c.Write(f)
	if err != nil {
		f.Close() // close, but only return/handle the write error
		return err
	}

	return f.Close()
}

// Write writes the csv.gzip to the Writer provided
func (c *Converter) Write(writer io.Writer) error {
	var zw io.WriteCloser
	var countRows int64
	var err error
	writeRow := true
	rows := c.rows

	// Same size as sqlRowBatch
	var csvBuffer bytes.Buffer

	// CSV writer to csvBuffer
	csvWriter := csv.NewWriter(&csvBuffer)

	// Set delimiter
	if c.Delimiter != '\x00' {
		csvWriter.Comma = c.Delimiter
	}

	// Set headers
	columnNames, totalColumns, err := c.setCSVHeaders()
	if err != nil {
		return err
	}

	// GZIP writer to underline file.csv.gzip
	if c.SingleThreaded {
		zw, err = gzip.NewWriterLevel(writer, c.CompressionLevel)
	} else {
		zw, err = pgzip.NewWriterLevel(writer, c.CompressionLevel)
	}
	if err != nil {
		return err
	}
	defer zw.Close()

	// Buffer size: string bytes x sqlBatchSize x No. of Columns
	sqlBatchSize := c.getSqlBatchSize(totalColumns)

	sqlRowBatch := make([][]string, 0, sqlBatchSize)

	sqlRowBatch = append(sqlRowBatch, columnNames)

	// Buffers for each iteration
	values := make([]interface{}, totalColumns, totalColumns)
	valuePtrs := make([]interface{}, totalColumns, totalColumns)

	for i := range columnNames {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := c.stringify(values)

		if c.rowPreProcessor != nil {
			writeRow, row = c.rowPreProcessor(row, columnNames)
		}

		if writeRow {
			sqlRowBatch = append(sqlRowBatch, row)
			if len(sqlRowBatch) >= c.SqlBatchSize {
				countRows = countRows + int64(len(sqlRowBatch))
				// Convert from sql to csv
				// Writes to buffer
				err = csvWriter.WriteAll(sqlRowBatch)
				if err != nil {
					return err
				}

				// Convert from csv to gzip
				// Writes from buffer to underlying file
				_, err = zw.Write(csvBuffer.Bytes())
				if err != nil {
					return err
				}

				// Reset buffer
				sqlRowBatch = sqlRowBatch[:0]
				csvBuffer.Reset()
			}
		}
	}
	err = rows.Err()
	if err != nil {
		return err
	}

	// Flush the remaining buffer to file.
	countRows = countRows + int64(len(sqlRowBatch))
	err = csvWriter.WriteAll(sqlRowBatch)
	if err != nil {
		return err
	}
	_, err = zw.Write(csvBuffer.Bytes())
	if err != nil {
		return err
	}

	//Wipe the buffer
	sqlRowBatch = nil
	csvBuffer.Reset()

	// Log the total number of rows processed.
	log.Println("Total number of sql rows processed: ", countRows)

	return nil
}

func (c *Converter) setCSVHeaders() ([]string, int, error) {
	var headers []string
	columnNames, err := c.rows.Columns()
	if err != nil {
		return nil, 0, err
	}

	if c.WriteHeaders {
		// use Headers if set, otherwise default to
		// query Columns
		if len(c.Headers) > 0 {
			headers = c.Headers
		} else {
			headers = columnNames
		}
	}

	return headers, len(headers), nil
}

func (c *Converter) stringify(values []interface{}) []string {
	row := make([]string, len(values), len(values))
	var value interface{}

	for i := range values {
		rawValue := values[i]

		byteArray, ok := rawValue.([]byte)
		if ok {
			value = string(byteArray)
		} else {
			value = rawValue
		}

		timeValue, ok := value.(time.Time)
		if ok && c.TimeFormat != "" {
			value = timeValue.Format(c.TimeFormat)
		}

		if value == nil {
			row[i] = ""
		} else {
			row[i] = fmt.Sprintf("%v", value)
		}
	}

	return row
}

func (c *Converter) getSqlBatchSize(totalColumns int) int {
	// Use sqlBatchSize set by user
	if c.SqlBatchSize != 0 {
		return c.SqlBatchSize
	}
	// Default to 4096
	c.SqlBatchSize = 4096

	// Use Default value when Single thread.
	if c.SingleThreaded {
		return c.SqlBatchSize
	}

	// If Multi-threaded, then block size should be atleast 1Mb = 1048576 bytes
	// See https://github.com/klauspost/pgzip

	// (String X SqlBatchSize X TotalColumns) > 1048576
	// String = 16 bytes
	// (SqlBatchSize X TotalColumns) > 65536

	for (c.SqlBatchSize * totalColumns) > 65536 {
		c.SqlBatchSize = c.SqlBatchSize + c.SqlBatchSize
	}

	return c.SqlBatchSize
}

// New will return a Converter which will write your CSV however you like
// but will allow you to set a bunch of non-default behaivour like overriding
// headers or injecting a pre-processing step into your conversion
func New(rows *sql.Rows) *Converter {
	return &Converter{
		rows:             rows,
		WriteHeaders:     true,
		Delimiter:        ',',
		CompressionLevel: flate.DefaultCompression,
	}
}
