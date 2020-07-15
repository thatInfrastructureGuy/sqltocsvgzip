// sqltocsvgzip package converts database query results
// (in the form of database/sql Rows) into CSV.GZIP output.
//
// Source and README at https://github.com/thatInfrastructureGuy/sqltocsvgzip
package sqltocsvgzip

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"sync"
)

// WriteFile will write a CSV.GZIP file to the file name specified (with headers)
// based on whatever is in the sql.Rows you pass in. It calls WriteCsvToWriter under
// the hood.
func WriteFile(csvGzipFileName string, rows *sql.Rows) error {
	return New(rows).WriteFile(csvGzipFileName)
}

func UploadToS3(rows *sql.Rows) error {
	return DefaultConfig(rows).Upload()
}

// WriteFile writes the csv.gzip to the filename specified, return an error if problem
func (c *Converter) Upload() error {
	if c.UploadPartSize < minFileSize {
		return fmt.Errorf("UploadPartSize should be greater than %v\n", minFileSize)
	}

	// Create MultiPart S3 Upload
	err := c.createS3Session()
	if err != nil {
		return err
	}

	err = c.createMultipartRequest()
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	buf := bytes.Buffer{}
	c.uploadQ = make(chan *obj, c.UploadThreads)
	c.quit = make(chan bool, 1)

	// Upload Parts to S3
	for i := 0; i < c.UploadThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = c.UploadAndDeletePart()
			if err != nil {
				c.writeLog(Error, fmt.Sprintf(err.Error()))
			}
		}()
	}

	err = c.Write(&buf)
	if err != nil {
		// Abort S3 Upload
		awserr := c.abortMultipartUpload()
		if awserr != nil {
			return awserr
		}
		return err
	}

	wg.Wait()
	close(c.uploadQ)

	if c.partNumber == 0 {
		// Upload one time
		c.writeLog(Debug, fmt.Sprintf("Gzip files < 5 MB are uploaded together without batching."))
		err = c.UploadObjectToS3(&buf)
		if err != nil {
			return err
		}
		c.abortMultipartUpload()
		return nil
	}

	// Sort completed parts
	c.sortCompletedParts()
	// Complete S3 upload
	completeResponse, err := c.completeMultipartUpload()
	if err != nil {
		return err
	}
	uploadPath, err := url.PathUnescape(completeResponse.String())
	if err != nil {
		return err
	}
	c.writeLog(Info, fmt.Sprintf("Successfully uploaded file: %s\n", uploadPath))

	return nil
}

// WriteFile writes the csv.gzip to the filename specified, return an error if problem
func (c *Converter) WriteFile(csvGzipFileName string) error {
	f, err := os.Create(csvGzipFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	// Explicitely unset s3 upload
	c.S3Upload = false

	err = c.Write(f)
	if err != nil {
		return err
	}

	return nil
}

// Write writes the csv.gzip to the Writer provided
func (c *Converter) Write(w io.Writer) error {
	var countRows int64
	gzipBuffer := make([]byte, 0, c.UploadPartSize)
	writeRow := true

	csvWriter, csvBuffer := c.getCSVWriter()

	// Set headers
	columnNames, totalColumns, err := c.setCSVHeaders()
	if err != nil {
		return err
	}

	// Create slice and append headers
	c.getSqlBatchSize(totalColumns)
	sqlRowBatch := make([][]string, 0, c.SqlBatchSize)
	sqlRowBatch = append(sqlRowBatch, columnNames)

	// Buffers for each iteration
	values := make([]interface{}, totalColumns, totalColumns)
	valuePtrs := make([]interface{}, totalColumns, totalColumns)

	for i := range columnNames {
		valuePtrs[i] = &values[i]
	}

	// GZIP writer to underline file.csv.gzip
	zw, err := c.getGzipWriter(w)
	if err != nil {
		return err
	}
	defer zw.Close()

	// Iterate over sql rows
	for c.rows.Next() {
		select {
		case <-c.quit:
			return fmt.Errorf("Received quit signal. Exiting.")
		default:
			// Do nothing
		}

		if err = c.rows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := c.stringify(values)

		if c.rowPreProcessor != nil {
			writeRow, row = c.rowPreProcessor(row, columnNames)
		}

		if writeRow {
			sqlRowBatch = append(sqlRowBatch, row)

			// Write to CSV Buffer
			if len(sqlRowBatch) >= c.SqlBatchSize {
				c.writeLog(Debug, fmt.Sprintf("Batching at %v rows", len(sqlRowBatch)))
				countRows = countRows + int64(len(sqlRowBatch))
				err = csvWriter.WriteAll(sqlRowBatch)
				if err != nil {
					return err
				}
				// Reset Slice
				sqlRowBatch = sqlRowBatch[:0]
			}

			// Convert from csv to gzip
			// Writes from buffer to underlying file
			if csvBuffer.Len() >= c.UploadPartSize {
				bytesWritten, err := zw.Write(csvBuffer.Bytes())
				c.writeLog(Debug, fmt.Sprintf("Csv to gzip bytes written: %v", bytesWritten))
				if err != nil {
					return err
				}

				// Reset buffer
				csvBuffer.Reset()

			}
			// Upload partially created file to S3
			// If size of the gzip file exceeds maxFileStorage
			if c.S3Upload {
				tmpBuf, ok := w.(*bytes.Buffer)
				if !ok {
					return fmt.Errorf("Expected buffer. Got %T", w)
				}
				gzipBuffer = append(gzipBuffer, tmpBuf.Bytes()...)
				tmpBuf.Reset()

				c.writeLog(Debug, fmt.Sprintf("gzipBuffer size: %v", len(gzipBuffer)))
				if len(gzipBuffer) >= c.UploadPartSize {
					if c.partNumber == 10000 {
						return fmt.Errorf("Number of parts cannot exceed 10000. Please increase UploadPartSize and try again.")
					}

					// Add to Queue
					c.AddToQueue(bytes.NewBuffer(gzipBuffer))
				}
			}
		}
	}
	err = c.rows.Err()
	if err != nil {
		return err
	}

	// Flush the remaining buffer to file.
	countRows = countRows + int64(len(sqlRowBatch))
	err = csvWriter.WriteAll(sqlRowBatch)
	if err != nil {
		return err
	}
	//Wipe the buffer
	sqlRowBatch = nil

	_, err = zw.Write(csvBuffer.Bytes())
	if err != nil {
		return err
	}
	//Wipe the buffer
	csvBuffer.Reset()

	// Log the total number of rows processed.
	c.writeLog(Info, fmt.Sprintf("Total rows processed (sql rows + headers row): %v", countRows))

	// Upload last part of the file to S3
	if c.S3Upload {
		if c.partNumber == 0 {
			return nil
		}
		// Add to Queue for multipart upload
		tmpBuffer, ok := w.(*bytes.Buffer)
		if !ok {
			return fmt.Errorf("Expected buffer. Got %T", w)
		}
		c.AddToQueue(tmpBuffer)
	}

	return nil
}

func (c *Converter) AddToQueue(buf *bytes.Buffer) {
	// Increament PartNumber
	c.partNumber++

	if buf.Len() >= c.UploadPartSize {
		if c.partNumber > 1 {
			// Add part to queue
			c.writeLog(Debug, fmt.Sprintf("Add part to queue: #%v", c.partNumber))
			c.uploadQ <- &obj{
				partNumber: c.partNumber - 1,
				buf:        c.gzipBuf.Bytes(),
			}
		}

		c.gzipBuf = buf
	} else {
		c.writeLog(Debug, fmt.Sprintf("Buffer len %v should be greater than %v for upload.", buf.Len(), c.UploadPartSize))
		c.gzipBuf.Write(buf.Bytes())

		// Add part to queue
		c.writeLog(Debug, fmt.Sprintf("Add part to queue: #%v", c.partNumber))
		c.uploadQ <- &obj{
			partNumber: c.partNumber - 1,
			buf:        c.gzipBuf.Bytes(),
		}

		c.partNumber--
	}

	c.writeLog(Debug, fmt.Sprintf("c.gzipBuf:  %v at partNumber: %v", c.gzipBuf.Len(), c.partNumber))
	buf.Reset()
}

func (c *Converter) UploadAndDeletePart() (err error) {
	mu := &sync.RWMutex{}
	for s3obj := range c.uploadQ {
		err = c.uploadPart(s3obj.partNumber, s3obj.buf, mu)
		if err != nil {
			c.writeLog(Error, fmt.Sprintf("Error occurred. Sending quit signal to writer."))
			c.quit <- true
			c.abortMultipartUpload()
			return err
		}
	}
	c.writeLog(Debug, fmt.Sprintf("Received closed signal"))
	return
}

func (c *Converter) writeLog(logLevel LogLevel, logLine string) {
	if logLevel <= c.LogLevel {
		log.Println(logLine)
	}
}
