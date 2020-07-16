package sqltocsvgzip

import (
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/pgzip"
)

// getSqlBatchSize gets the size of rows to be retrieved.
// This batch is worked upon entirely before flushing to disk.
func (c *Converter) getSqlBatchSize(totalColumns int) {
	// Use sqlBatchSize set by user
	if c.SqlBatchSize != 0 {
		return
	}

	// Default to 4096
	c.SqlBatchSize = 4096
}

func (c *Converter) getGzipWriter(writer io.Writer) (io.WriteCloser, error) {
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
