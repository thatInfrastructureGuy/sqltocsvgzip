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
	var buf bytes.Buffer
	wg := sync.WaitGroup{}

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

	// Upload Parts to S3
	c.uploadQ = make(chan *obj, c.UploadThreads)

	for i := 0; i < c.UploadThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = c.UploadAndDeletePart()
			if err != nil {
				log.Println(err)
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
	log.Printf("Successfully uploaded file: %s\n", uploadPath)

	return nil
}

// WriteFile writes the csv.gzip to the filename specified, return an error if problem
func (c *Converter) WriteFile(csvGzipFileName string) error {
	f, err := os.Create(csvGzipFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	err = c.Write(f)
	if err != nil {
		return err
	}

	return nil
}

// Write writes the csv.gzip to the Writer provided
func (c *Converter) Write(w io.Writer) error {
	var countRows, partNumber, gzipBytes int64
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
		if err = c.rows.Scan(valuePtrs...); err != nil {
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
				// Reset buffer
				sqlRowBatch = sqlRowBatch[:0]

				// Convert from csv to gzip
				// Writes from buffer to underlying file
				if csvBuffer.Len() >= 1.5*1024*1024 {
					bytesWritten, err := zw.Write(csvBuffer.Bytes())
					if err != nil {
						return err
					}
					gzipBytes = gzipBytes + int64(bytesWritten)
					// Reset buffer
					csvBuffer.Reset()
				}
			}

			// Upload partially created file to S3
			// If UploadtoS3 is set to true &&
			// If size of the gzip file exceeds maxFileStorage
			if c.S3Upload {
				select {
				case <-c.quit:
					return fmt.Errorf("Received quit signal. Exiting.")
				default:
					// Do nothing
				}

				if gzipBytes >= c.UploadPartSize {
					if partNumber == 10000 {
						return fmt.Errorf("Number of parts cannot exceed 10000. Please increase UploadPartSize and try again.")
					}
					// Increament PartNumber
					partNumber++
					// Add to Queue
					c.AddToQueue(w, partNumber)
					if err != nil {
						return err
					}
					gzipBytes = 0
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

	// Upload last part of the file to S3
	if c.S3Upload {
		// Increament PartNumber
		partNumber++
		if partNumber == 1 {
			// Upload one time
			if c.Debug {
				log.Println("Gzip files < 5 MB are uploaded together without batching.")
			}
			err = c.UploadObjectToS3(w)
			if err != nil {
				return err
			}
			c.abortMultipartUpload()
		} else {
			// Add to Queue for multipart upload
			c.AddToQueue(w, partNumber)
			if err != nil {
				return err
			}
		}
		close(c.uploadQ)
	}

	// Log the total number of rows processed.
	log.Println("Total number of sql rows processed: ", countRows)

	return nil
}

func (c *Converter) AddToQueue(w io.Writer, partNumber int64) error {
	buf, ok := w.(*bytes.Buffer)
	if !ok {
		return fmt.Errorf("Expected buffer. Got %T", w)
	}

	if buf.Len() < minFileSize {
		buf.Grow(minFileSize - buf.Len())
	}

	// Add previous part to queue
	if c.Debug {
		log.Println("Add part to queue: #", partNumber)
	}
	c.uploadQ <- &obj{
		partNumber: partNumber,
		buf:        buf.Bytes(),
	}
	buf.Reset()

	return nil
}

func (c *Converter) UploadAndDeletePart() (err error) {
	mu := &sync.RWMutex{}
	for s3obj := range c.uploadQ {
		err = c.uploadPart(s3obj.partNumber, s3obj.buf, mu)
		if err != nil {
			log.Println("Error occurred. Sending quit signal to writer.")
			c.quit <- true
			c.abortMultipartUpload()
			return err
		}
	}
	if c.Debug {
		log.Println("Received closed signal")
	}
	return
}
