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
	"io/ioutil"
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

func UploadToS3(csvGzipFileName string, rows *sql.Rows) error {
	return DefaultConfig(rows).WriteFile(csvGzipFileName)
}

// WriteFile writes the csv.gzip to the filename specified, return an error if problem
func (c *Converter) WriteFile(csvGzipFileName string) error {
	f, err := os.Create(csvGzipFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	wg := sync.WaitGroup{}
	quit := make(chan bool, 1)

	// Create MultiPart S3 Upload
	if c.S3Upload {
		err = c.createS3Session()
		if err != nil {
			return err
		}

		err = c.createMultipartRequest()
		if err != nil {
			return err
		}

		// Upload Parts to S3
		c.s3Uploadable = make(chan *s3Obj, c.S3UploadThreads)

		for i := 0; i < c.S3UploadThreads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = c.UploadAndDeletePart(quit)
				if err != nil {
					log.Println(err)
				}
			}()
		}
	}

	err = c.Write(f, quit)
	if err != nil {
		// Abort S3 Upload
		if c.S3Upload {
			awserr := c.abortMultipartUpload()
			if awserr != nil {
				return awserr
			}
		}
		return err
	}

	if c.S3Upload {
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
	}

	return nil
}

// Write writes the csv.gzip to the Writer provided
func (c *Converter) Write(f *os.File, quit chan bool) error {
	if c.S3Upload && c.S3UploadMaxPartSize < minFileSize {
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
	if c.Debug {
		log.Println("SQL Batch size: ", sqlBatchSize)
	}

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
				select {
				case <-quit:
					return fmt.Errorf("Received quit signal. Exiting.")
				default:
					// Do nothing
				}

				fileInfo, err := f.Stat()
				if err != nil {
					return err
				}

				if fileInfo.Size() >= c.S3UploadMaxPartSize {
					if partNumber > 10000 {
						return fmt.Errorf("Number of parts cannot exceed 10000")
					}
					// Increament PartNumber
					partNumber++
					// Add to Queue
					partNumber, err = c.AddToQueue(f, partNumber)
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
			if c.Debug {
				log.Println("Gzip files < 5 MB are uploaded together without batching.")
			}
			err = c.UploadObjectToS3(f)
			if err != nil {
				return err
			}
			c.abortMultipartUpload()
		} else {
			// Add to Queue for multipart upload
			partNumber, err = c.AddToQueue(f, partNumber)
			if err != nil {
				return err
			}
		}
		close(c.s3Uploadable)
	}

	// Log the total number of rows processed.
	log.Println("Total number of sql rows processed: ", countRows)

	return nil
}

func (c *Converter) AddToQueue(f *os.File, partNumber int64) (newPartNumber int64, err error) {
	newPartNumber = partNumber

	buf, err := ioutil.ReadFile(f.Name())
	if err != nil {
		return 0, err
	}

	if c.Debug {
		log.Printf("Part %v wrote bytes %v.\n", partNumber, len(buf))
	}

	if len(buf) >= minFileSize {
		// Add previous part to queue
		if partNumber > 1 {
			if c.Debug {
				log.Println("Add part to queue: #", partNumber-1)
			}
			c.s3Uploadable <- &s3Obj{
				partNumber: partNumber - 1,
				buf:        c.gzipBuf,
			}
		}

		c.gzipBuf = buf
	} else {
		if c.Debug {
			log.Printf("Part size is less than %v. Merging with previous part.\n", minFileSize)
		}
		// Write the bytes to previous partFile
		c.gzipBuf = append(c.gzipBuf, buf...)
		log.Println("Add part to queue: #", partNumber-1)
		c.s3Uploadable <- &s3Obj{
			partNumber: partNumber - 1,
			buf:        c.gzipBuf,
		}
		newPartNumber = partNumber - 1
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

func (c *Converter) UploadAndDeletePart(quit chan bool) (err error) {
	mu := &sync.RWMutex{}
	for s3obj := range c.s3Uploadable {
		err = c.uploadPart(s3obj.partNumber, s3obj.buf, mu)
		if err != nil {
			log.Println("Error occurred. Sending quit signal to writer.")
			quit <- true
			c.abortMultipartUpload()
			return err
		}
	}
	if c.Debug {
		log.Println("Received closed signal")
	}
	return
}
