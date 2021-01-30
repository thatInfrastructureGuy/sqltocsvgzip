package sqltocsvgzip

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sync"
)

type csvBuf struct {
	data     []byte
	lastPart bool
}

func (c *Converter) getCSVWriter() (*csv.Writer, *bytes.Buffer) {
	// Same size as sqlRowBatch
	csvBuffer := bytes.NewBuffer(make([]byte, 0, c.CsvBufferSize))

	// CSV writer to csvBuffer
	csvWriter := csv.NewWriter(csvBuffer)

	// Set delimiter
	if c.Delimiter != '\x00' {
		csvWriter.Comma = c.Delimiter
	}

	return csvWriter, csvBuffer
}

func (c *Converter) getCSVHeaders(csvWriter *csv.Writer) (headers []string, err error) {
	columnNames, err := c.rows.Columns()
	if err != nil {
		return nil, err
	}

	// use Headers if set, otherwise default to
	// query Columns
	if len(c.Headers) > 0 {
		headers = c.Headers
	} else {
		headers = columnNames
	}

	// Write to CSV Buffer
	if c.WriteHeaders {
		err = csvWriter.Write(headers)
		if err != nil {
			return nil, err
		}
		csvWriter.Flush()
	}

	return
}

func (c *Converter) rowToCSV(getHeaders, toCSV chan []string, toGzip chan csvBuf, wg *sync.WaitGroup) {
	defer wg.Done()
	csvWriter, csvBuffer := c.getCSVWriter()

	// Get headers
	columnNames, err := c.getCSVHeaders(csvWriter)
	if err != nil {
		close(toGzip)
		c.quit <- fmt.Errorf("Error setting CSV Headers: %v", err)
		return
	}

	getHeaders <- columnNames

	for row := range toCSV {
		c.RowCount = c.RowCount + 1

		// Write to CSV Buffer
		err = csvWriter.Write(row)
		if err != nil {
			close(toGzip)
			c.quit <- fmt.Errorf("Error writing to csv buffer: %v", err)
			return
		}
		csvWriter.Flush()

		// Convert from csv to gzip
		if csvBuffer.Len() >= (c.GzipBatchPerGoroutine * c.GzipGoroutines) {
			toGzip <- csvBuf{
				data:     csvBuffer.Bytes(),
				lastPart: false,
			}

			// Reset buffer
			csvBuffer.Reset()
		}
	}

	// Flush remaining buffer contents to gzip
	toGzip <- csvBuf{
		data:     csvBuffer.Bytes(),
		lastPart: true,
	}

	// Reset buffer
	csvBuffer.Reset()

	close(toGzip)
}
