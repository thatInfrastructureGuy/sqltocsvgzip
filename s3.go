package sqltocsvgzip

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const maxRetries = 3

func (c *Converter) createMultipartRequest(file *os.File) (err error) {
	// Filetype ref: https://mimesniff.spec.whatwg.org/#matching-an-archive-type-pattern
	fileType := "application/x-gzip"

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(c.S3Bucket),
		Key:         aws.String(c.S3Path),
		ACL:         aws.String(c.S3Acl),
		ContentType: aws.String(fileType),
	}

	c.S3Resp, err = c.S3Svc.CreateMultipartUpload(input)
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok {
			return awserr
		}
		return err
	}

	fmt.Println("Created multipart upload request")
	return nil
}

// createS3Session authenticates with AWS and returns a S3 client
func (c *Converter) createS3Session() error {
	if len(c.S3Bucket) == 0 || len(c.S3Region) == 0 {
		return fmt.Errorf("Both S3Bucket and S3Region variables needed to upload file to AWS S3")
	}
	if len(c.S3Acl) == 0 {
		c.S3Acl = "bucket-owner-full-control"
	}

	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(c.S3Region),
	}))

	c.S3Svc = s3.New(sess)

	return nil
}

func (c *Converter) abortMultipartUpload() error {
	fmt.Println("Aborting multipart upload for UploadId#" + *c.S3Resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   c.S3Resp.Bucket,
		Key:      c.S3Resp.Key,
		UploadId: c.S3Resp.UploadId,
	}
	_, err := c.S3Svc.AbortMultipartUpload(abortInput)
	return err
}

func (c *Converter) completeMultipartUpload() (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   c.S3Resp.Bucket,
		Key:      c.S3Resp.Key,
		UploadId: c.S3Resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: c.S3CompletedParts,
		},
	}
	return c.S3Svc.CompleteMultipartUpload(completeInput)
}

func (c *Converter) uploadPart(file *os.File, partNumber int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          file,
		Bucket:        c.S3Resp.Bucket,
		Key:           c.S3Resp.Key,
		PartNumber:    aws.Int64(partNumber),
		UploadId:      c.S3Resp.UploadId,
		ContentLength: aws.Int64(fileInfo.Size()),
	}

	for tryNum <= maxRetries {
		uploadResult, err := c.S3Svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return aerr
				}
				return err
			}
			fmt.Printf("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			fmt.Printf("Uploaded part #%v\n", partNumber)
			c.S3CompletedParts = append(c.S3CompletedParts, &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(partNumber),
			})
			return nil
		}
	}
	return nil
}
