// sqltocsvgzip package converts database query results
// (in the form of database/sql Rows) into CSV.GZIP output.
//
// Source and README at https://github.com/thatInfrastructureGuy/sqltocsvgzip
package sqltocsvgzip

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/pgzip"
)

// WriteFile will write a CSV.GZIP file to the file name specified (with headers)
// based on whatever is in the sql.Rows you pass in. It calls WriteCsvToWriter under
// the hood.
func WriteFile(csvGzipFileName string, rows *sql.Rows) error {
	return New(rows).WriteFile(csvGzipFileName)
}

func UploadToS3(csvGzipFileName string, rows *sql.Rows) error {
	return DefaultConfig(rows).WriteFile(csvGzipFileName)
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

// WriteFile writes the csv.gzip to the filename specified, return an error if problem
func (c *Converter) WriteFile(csvGzipFileName string) error {
	f, err := os.Create(csvGzipFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	// Create MultiPart S3 Upload
	if c.S3Upload {
		err = c.createS3Session()
		if err != nil {
			return err
		}

		err = c.createMultipartRequest(f)
		if err != nil {
			return err
		}

		c.UploadAndDeletePart()
	}

	go func(f *os.File) {
		err = c.Write(f)
		if err != nil {
			// Abort S3 Upload
			if c.S3Upload {
				awserr := c.abortMultipartUpload()
				if awserr != nil {
					log.Println(awserr)
				}
			}
			log.Println(err)
		}
	}(f)

	// Complete S3 upload
	if c.S3Upload {
		completeResponse, err := c.completeMultipartUpload()
		if err != nil {
			awserr := c.abortMultipartUpload()
			if awserr != nil {
				log.Println(awserr)
			}
			return err
		}
		log.Printf("Successfully uploaded file: %s\n", completeResponse.String())
	}

	return nil
}

// Write writes the csv.gzip to the Writer provided
func (c *Converter) Write(f *os.File) error {
	log.Println("S3UploadMaxPartSize is: ", c.S3UploadMaxPartSize)
	if c.S3UploadMaxPartSize < minFileSize {
		return fmt.Errorf("S3UploadMaxPartSize should be greater than %v\n", minFileSize)
	}
	var countRows, partNumber int64
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
	zw, err := c.selectCompressionMethod(f)
	if err != nil {
		return err
	}
	defer zw.Close()

	// Buffer size: string bytes x sqlBatchSize x No. of Columns
	sqlBatchSize := c.getSqlBatchSize(totalColumns)

	// Create buffer
	sqlRowBatch := make([][]string, 0, sqlBatchSize)

	// Append headers
	sqlRowBatch = append(sqlRowBatch, columnNames)

	// Buffers for each iteration
	values := make([]interface{}, totalColumns, totalColumns)
	valuePtrs := make([]interface{}, totalColumns, totalColumns)

	for i := range columnNames {
		valuePtrs[i] = &values[i]
	}

	// Iterate over sql rows
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
			if len(sqlRowBatch) >= sqlBatchSize {
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

			// Upload partially created file to S3
			// If UploadtoS3 is set to true &&
			// If size of the gzip file exceeds maxFileStorage
			if c.S3Upload {
				fileInfo, err := f.Stat()
				if err != nil {
					return err
				}
				fileSize := fileInfo.Size()

				if fileSize >= c.S3UploadMaxPartSize && partNumber < 10000 {
					// Increament PartNumber
					partNumber++
					// Add to Queue
					log.Println("Adding to Queue")
					partNumber, err = c.AddToQueue(f, partNumber, false)
					if err != nil {
						return err
					}
				}
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

	// Upload last part of the file to S3
	if c.S3Upload {
		// Increament PartNumber
		partNumber++
		if partNumber == 1 {
			// Upload one time
			err = c.UploadObjectToS3(f)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			// Add to Queue for multipart upload
			partNumber, err = c.AddToQueue(f, partNumber, true)
		}
		if err != nil {
			return err
		}
	}

	// Log the total number of rows processed.
	log.Println("Total number of sql rows processed: ", countRows)

	return nil
}

func (c *Converter) UploadObjectToS3(f *os.File) error {
	return fmt.Errorf("Method UploadObjectToS3 not implemented\n")
}

func (c *Converter) AddToQueue(f *os.File, partNumber int64, uploadLastPart bool) (newPartNumber int64, err error) {
	partFile := strconv.FormatInt(partNumber, 10)
	prevFile := strconv.FormatInt(partNumber-1, 10)
	newPartNumber = partNumber

	fcurr, err := os.Create(partFile)
	if err != nil {
		return 0, err
	}

	bytesWritten, err := io.Copy(fcurr, f)
	if err != nil {
		return 0, err
	}
	fcurr.Close()
	log.Printf("Part %v wrote bytes %v\n", partNumber, bytesWritten)

	if bytesWritten < minFileSize {
		log.Println("Will merge into prev file")
		// Write the bytes to previous partFile
		fprev, err := os.OpenFile(prevFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
		if err != nil {
			return 0, err
		}
		defer fprev.Close()

		bytesWritten, err = io.Copy(fprev, f)
		if err != nil {
			return 0, err
		}

		// Delete the current partFile
		err = os.Remove(partFile)
		if err != nil {
			return 0, err
		}

		newPartNumber = partNumber - 1

		if uploadLastPart {
			uploadLastPart = false
		}
	}

	// send prev to channel
	if partNumber > 1 {
		log.Println("Add part to queue: ", partNumber-1)
		c.S3Uploadable <- partNumber - 1
	}

	if uploadLastPart {
		log.Println("Add last part to queue: ", partNumber)
		c.S3Uploadable <- partNumber
		c.quit <- true
	}

	// Reset file
	err = f.Truncate(0)
	if err != nil {
		return 0, err
	}
	// Move the reader
	_, err = f.Seek(0, 0)
	if err != nil {
		return 0, err
	}

	return newPartNumber, nil
}

func (c *Converter) UploadAndDeletePart() {
	for {
		select {
		case partNumber := <-c.S3Uploadable:
			fileName := strconv.FormatInt(partNumber, 10)
			f, err := os.Open(fileName)
			if err != nil {
				log.Print(err)
				err = c.abortMultipartUpload()
			}
			log.Println("Uploading Part: ", partNumber)
			err = c.uploadPart(f, partNumber)
			f.Close()
			if err != nil {
				log.Print(err)
				err = c.abortMultipartUpload()
				return
			}

			err = os.Remove(fileName)
			if err != nil {
				log.Print(err)
			}
		case <-c.quit:
			log.Println("Received Quit signal.")
			return
		}
	}
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

// getSqlBatchSize gets the size of rows to be retrieved.
// This batch is worked upon entirely before flushing to disk.
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

	for (c.SqlBatchSize * totalColumns) <= 65536 {
		c.SqlBatchSize = c.SqlBatchSize * 2
	}

	// We aim for 1.5 MB - 2 MB to be on a safe side
	c.SqlBatchSize = c.SqlBatchSize * 2

	return c.SqlBatchSize
}

func (c *Converter) selectCompressionMethod(writer io.Writer) (io.WriteCloser, error) {
	// Use gzip if single threaded
	if c.SingleThreaded {
		zw, err := gzip.NewWriterLevel(writer, c.CompressionLevel)
		return zw, err
	}

	// Use pgzip if multi-threaded
	zw, err := pgzip.NewWriterLevel(writer, c.CompressionLevel)
	if err != nil {
		return zw, err
	}
	err = zw.SetConcurrency(c.GzipBatchPerGoroutine, c.GzipGoroutines)
	return zw, err
}
