package processors

import (
	"compress/gzip"
	"io/ioutil"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/bluebottlecoffee/ratchet/data"
)

type S3Writer struct {
	bucket  string
	key     string
	tmpfile *os.File
	gz      *gzip.Writer
	svc     *s3.S3
}

func NewS3Writer(awsRegion, bucket, key string) *S3Writer {
	w := S3Writer{bucket: bucket, key: key}

	tmpfile, err := ioutil.TempFile(os.TempDir(), strings.Replace(key, string(os.PathSeparator), "-", 3))

	if err != nil {
		panic(err)
	}
	w.tmpfile = tmpfile
	w.gz = gzip.NewWriter(tmpfile)

	mySession := session.New(&aws.Config{Region: aws.String(awsRegion)})
	w.svc = s3.New(mySession)

	return &w
}

func (w *S3Writer) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	if _, err := w.gz.Write(d); err != nil {
		killChan <- err
	}
}

func (w *S3Writer) Finish(outputChan chan data.JSON, killChan chan error) {
	var err error

	if err = w.gz.Close(); err != nil {
		killChan <- err
	}

	params := &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
		Body:   w.tmpfile,
	}
	_, err = w.svc.PutObject(params)

	if err != nil {
		killChan <- err
	}

	os.Remove(w.tmpfile.Name())
}

func (w *S3Writer) String() string {
	return "S3Writer"
}
