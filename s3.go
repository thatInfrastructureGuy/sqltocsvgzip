package sqltocsvgzip

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (c *Converter) createMultipartRequest(svc *s3.S3, file *os.File) (*s3.CreateMultipartUploadOutput, error) {
	// Filetype ref: https://mimesniff.spec.whatwg.org/#matching-an-archive-type-pattern
	fileType := "application/x-gzip"

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(c.S3Bucket),
		Key:         aws.String(c.S3Path),
		ACL:         aws.String(c.S3Acl),
		ContentType: aws.String(fileType),
	}

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		return nil, err
	}

	fmt.Println("Created multipart upload request")
	return resp, nil
}

// createS3Session authenticates with AWS and returns a S3 client
func (c *Converter) createS3Session() (*s3.S3, error) {
	if len(c.S3Bucket) == 0 || len(c.S3Region) == 0 {
		return nil, fmt.Errorf("Both S3Bucket and S3Region variables needed to upload file to AWS S3")
	}
	if len(c.S3Acl) == 0 {
		c.S3Acl = "bucket-owner-full-control"
	}

	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(c.S3Region),
	}))

	svc := s3.New(sess)

	return svc, nil
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	fmt.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	return svc.CompleteMultipartUpload(completeInput)
}
