package sqltocsvgzip

import (
	"sort"

	"github.com/aws/aws-sdk-go/service/s3"
)

type byPartNumber []*s3.CompletedPart

func (c *Converter) sortCompletedParts() {
	sort.Sort(byPartNumber(c.s3CompletedParts))
}

func (b byPartNumber) Len() int {
	return len(b)
}

func (b byPartNumber) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byPartNumber) Less(i, j int) bool {
	return (*b[i].PartNumber < *b[j].PartNumber)
}
