package sqltocsvgzip

import (
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/pgzip"
)

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
