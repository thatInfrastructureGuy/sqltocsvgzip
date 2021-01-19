package sqltocsvgzip

import (
	"fmt"
	"strconv"
	"time"
)

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

func (c *Converter) preProcessRows(toPreprocess chan []interface{}, columnNames []string, toCSV chan []string) {
	writeRow := true

	for values := range toPreprocess {
		row := c.stringify(values)

		if c.rowPreProcessor != nil {
			writeRow, row = c.rowPreProcessor(row, columnNames)
		}

		if writeRow {
			fmt.Println("[DEBUG] row:", row)
			toCSV <- row
		}
	}

	close(toCSV)
}
