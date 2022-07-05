package main

import (
	"compress/gzip"
	"context"
	"flag"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sync"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/peterbourgon/ff/v3"
)

// Flags .
type Flags struct {
	Bucket             string
	Config             string
	ContainerNameOrID  string
	DBName             string
	DBUser             string
	Endpoint           string
	Prefix             string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
}

func (f *Flags) Register(fs *flag.FlagSet) {
	fs.StringVar(&f.Config, "config", "", "config file")
	fs.StringVar(&f.ContainerNameOrID, "container", "", "container name or ID")
	fs.StringVar(&f.DBName, "db.name", "", "database name")
	fs.StringVar(&f.DBUser, "db.user", "", "database user")
	fs.StringVar(&f.Bucket, "s3.bucket", "", "bucket name")
	fs.StringVar(&f.Endpoint, "s3.endpoint", "", "s3 endpoint")
	fs.StringVar(&f.Prefix, "s3.prefix", "postgres-backups", "s3 object name prefix")
	fs.StringVar(&f.AWSAccessKeyID, "aws.access_key_id", "", "aws access key id")
	fs.StringVar(&f.AWSSecretAccessKey, "aws.secret_access_key", "", "aws secret access key")
}

func main() {
	fs := flag.NewFlagSet("docker-pg-backup", flag.ContinueOnError)
	var flags Flags
	flags.Register(fs)

	if err := ff.Parse(fs, os.Args[1:],
		ff.WithEnvVarPrefix("S3_BACKUP"),
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	); err != nil {
		log.Fatal(err)
	}
	if flags.AWSSecretAccessKey != "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", flags.AWSSecretAccessKey)
	}
	if flags.AWSAccessKeyID != "" {
		os.Setenv("AWS_ACCESS_KEY_ID", flags.AWSAccessKeyID)
	}

	ctx := context.Background()
	if err := Backup(ctx, flags); err != nil {
		log.Fatal(err)
	}

}

func Backup(ctx context.Context, flags Flags) error {

	u, err := url.Parse(flags.Endpoint)
	if err != nil {
		log.Fatalln(err)
	}

	var defaultAWSCredProviders = []credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.FileAWSCredentials{},
		&credentials.IAM{},
		&credentials.EnvMinio{},
	}

	creds := credentials.NewChainCredentials(defaultAWSCredProviders)

	mc, err := minio.New(u.Host, &minio.Options{
		Creds:        creds,
		Secure:       true,
		Region:       s3utils.GetRegionFromURL(*u),
		BucketLookup: minio.BucketLookupAuto,
	})
	if err != nil {
		log.Fatalln(err)
	}

	tmpDir, err := os.MkdirTemp("", "*-pg-backup")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			log.Println(err)
		}
	}()

	dumpName := filepath.Join(tmpDir, "/db.sql.gz")
	if err := DumpDB(ctx, dumpName, flags.ContainerNameOrID, flags.DBName, flags.DBUser); err != nil {
		return err
	}

	f, err := os.Open(dumpName)
	if err != nil {
		return err
	}
	defer f.Close()

	objectName := path.Join(flags.Prefix, flags.DBName, time.Now().UTC().Format("2006-01-02T15_04_05.999999999")+".sql.gz")
	if err != nil {
		return err
	}

	_, err = mc.PutObject(ctx, flags.Bucket, objectName, f, -1, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
}

func DumpDB(ctx context.Context, filename string, container string, db string, user string) error {

	args := []string{
		"exec", container,
		"pg_dump",
		"-U", user,
		db,
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	defer zw.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		io.Copy(zw, stdout)
	}()
	cmd.Run()
	wg.Wait()
	return nil

}
