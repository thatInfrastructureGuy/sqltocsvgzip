package sqltocsvgzip

import (
	"bytes"
	"encoding/csv"
	"fmt"
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

func (c *Converter) setCSVHeaders(csvWriter *csv.Writer) ([]string, error) {
	var headers []string
	columnNames, err := c.rows.Columns()
	if err != nil {
		return nil, err
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

	// Write to CSV Buffer
	err = csvWriter.Write(headers)
	if err != nil {
		return nil, err
	}
	csvWriter.Flush()

	return headers, nil
}

func (c *Converter) rowToCSV(toCSV chan []string, toGzip chan *csvBuf) {
	csvWriter, csvBuffer := c.getCSVWriter()
	// Set headers
	columnNames, err := c.setCSVHeaders(csvWriter)
	if err != nil {
		close(toGzip)
		c.Error = fmt.Errorf("Error setting CSV Headers: ", err)
		return
	}

	toCSV <- columnNames

	for row := range toCSV {
		c.RowCount = c.RowCount + 1

		// Write to CSV Buffer
		err = csvWriter.Write(row)
		if err != nil {
			close(toGzip)
			c.Error = fmt.Errorf("Error writing to csv buffer: ", err)
			return
		}
		csvWriter.Flush()

		// Convert from csv to gzip
		if csvBuffer.Len() >= (c.GzipBatchPerGoroutine * c.GzipGoroutines) {
			toGzip <- &csvBuf{
				data:     csvBuffer.Bytes(),
				lastPart: false,
			}

			// Reset buffer
			csvBuffer.Reset()
		}
	}

	// Flush remaining buffer contents to gzip
	toGzip <- &csvBuf{
		data:     csvBuffer.Bytes(),
		lastPart: true,
	}

	// Reset buffer
	csvBuffer.Reset()

	close(toGzip)
}
