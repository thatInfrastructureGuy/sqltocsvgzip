package sqltocsvgzip

import (
	"io"

	"github.com/klauspost/pgzip"
)

func (c *Converter) getGzipWriter(writer io.Writer) (*pgzip.Writer, error) {
	// Use pgzip for multi-threaded
	zw, err := pgzip.NewWriterLevel(writer, c.CompressionLevel)
	if err != nil {
		return zw, err
	}
	err = zw.SetConcurrency(c.GzipBatchPerGoroutine, c.GzipGoroutines)
	return zw, err
}
