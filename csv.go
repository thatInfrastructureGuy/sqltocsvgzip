package sqltocsvgzip

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"time"
)

func (c *Converter) getCSVWriter() (*csv.Writer, *bytes.Buffer) {
	// Same size as sqlRowBatch
	var csvBuffer bytes.Buffer

	// CSV writer to csvBuffer
	csvWriter := csv.NewWriter(&csvBuffer)

	// Set delimiter
	if c.Delimiter != '\x00' {
		csvWriter.Comma = c.Delimiter
	}

	return csvWriter, &csvBuffer
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

	for i, rawValue := range values {
		if rawValue == nil {
			row[i] = ""
			continue
		}

		byteArray, ok := rawValue.([]byte)
		if ok {
			rawValue = string(byteArray)
		}

		switch castValue := rawValue.(type) {
		case time.Time:
			if c.TimeFormat != "" {
				row[i] = castValue.Format(c.TimeFormat)
			}
		case bool:
			row[i] = strconv.FormatBool(castValue)
		case string:
			row[i] = castValue
		case int:
			row[i] = strconv.FormatInt(int64(castValue), 10)
		case int8:
			row[i] = strconv.FormatInt(int64(castValue), 10)
		case int16:
			row[i] = strconv.FormatInt(int64(castValue), 10)
		case int32:
			row[i] = strconv.FormatInt(int64(castValue), 10)
		case int64:
			row[i] = strconv.FormatInt(int64(castValue), 10)
		case uint:
			row[i] = strconv.FormatUint(uint64(castValue), 10)
		case uint8:
			row[i] = strconv.FormatUint(uint64(castValue), 10)
		case uint16:
			row[i] = strconv.FormatUint(uint64(castValue), 10)
		case uint32:
			row[i] = strconv.FormatUint(uint64(castValue), 10)
		case uint64:
			row[i] = strconv.FormatUint(uint64(castValue), 10)
		default:
			row[i] = fmt.Sprintf("%v", castValue)
		}
	}

	return row
}
