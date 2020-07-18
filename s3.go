package sqltocsvgzip

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const maxRetries = 3

func (c *Converter) createMultipartRequest() (err error) {
	// Filetype ref: https://mimesniff.spec.whatwg.org/#matching-an-archive-type-pattern
	fileType := "application/x-gzip"

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(c.S3Bucket),
		Key:         aws.String(c.S3Path),
		ACL:         aws.String(c.S3Acl),
		ContentType: aws.String(fileType),
	}

	c.s3Resp, err = c.s3Svc.CreateMultipartUpload(input)
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok {
			return awserr
		}
		return err
	}

	c.writeLog(Info, "Created multipart upload request.")
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

	c.s3Svc = s3.New(sess)

	return nil
}

func (c *Converter) abortMultipartUpload() error {
	c.writeLog(Info, "Aborting multipart upload for UploadId: "+aws.StringValue(c.s3Resp.UploadId))
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   c.s3Resp.Bucket,
		Key:      c.s3Resp.Key,
		UploadId: c.s3Resp.UploadId,
	}
	_, err := c.s3Svc.AbortMultipartUpload(abortInput)
	return err
}

func (c *Converter) completeMultipartUpload() (*s3.CompleteMultipartUploadOutput, error) {
	c.writeLog(Info, "Completing multipart upload for UploadId: "+aws.StringValue(c.s3Resp.UploadId))
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   c.s3Resp.Bucket,
		Key:      c.s3Resp.Key,
		UploadId: c.s3Resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: c.s3CompletedParts,
		},
	}
	return c.s3Svc.CompleteMultipartUpload(completeInput)
}

func (c *Converter) uploadPart(partNumber int64, buf []byte, mu *sync.RWMutex) (err error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:       bytes.NewReader(buf),
		Bucket:     c.s3Resp.Bucket,
		Key:        c.s3Resp.Key,
		PartNumber: aws.Int64(partNumber),
		UploadId:   c.s3Resp.UploadId,
	}

	for tryNum <= maxRetries {
		uploadResult, err := c.s3Svc.UploadPart(partInput)
		if err != nil {
			c.writeLog(Error, err.Error())
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return aerr
				}
				return err
			}
			c.writeLog(Info, fmt.Sprintf("Retrying to upload part: #%v", partNumber))
			tryNum++
		} else {
			c.writeLog(Info, fmt.Sprintf("Uploaded part: #%v", partNumber))
			mu.Lock()
			c.s3CompletedParts = append(c.s3CompletedParts, &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(partNumber),
			})
			mu.Unlock()
			return nil
		}
	}
	return nil
}

// UploadObjectToS3 uploads a file to AWS S3 without batching.
func (c *Converter) UploadObjectToS3(w io.Writer) error {
	buf, ok := w.(*bytes.Buffer)
	if !ok {
		return fmt.Errorf("Expected buffer. Got %T", w)
	}

	fileType := "application/x-gzip"

	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(c.S3Region),
	}))

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	// Upload the file to S3.
	res, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(c.S3Bucket),
		Key:         aws.String(c.S3Path),
		ACL:         aws.String(c.S3Acl),
		ContentType: aws.String(fileType),
		Body:        bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		return err
	}

	uploadPath, err := url.PathUnescape(res.Location)
	if err != nil {
		return err
	}
	c.writeLog(Info, "Successfully uploaded file: "+uploadPath)
	return nil
}
