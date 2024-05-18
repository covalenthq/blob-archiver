package storage

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"slices"
	"path"

	"golang.org/x/sync/errgroup"

	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/base-org/blob-archiver/common/flags"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Storage struct {
	s3       *minio.Client
	bucket   string
	prefix   string
	log      log.Logger
	compress bool
}

func NewS3Storage(cfg flags.S3Config, l log.Logger) (*S3Storage, error) {
	var c *credentials.Credentials
	if cfg.S3CredentialType == flags.S3CredentialStatic {
		c = credentials.NewStaticV4(cfg.AccessKey, cfg.SecretAccessKey, "")
	} else {
		c = credentials.NewIAM("")
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  c,
		Secure: cfg.UseHttps,
	})

	if err != nil {
		return nil, err
	}

	return &S3Storage{
		s3:       client,
		bucket:   cfg.Bucket,
		prefix:   cfg.Prefix,
		log:      l,
		compress: cfg.Compress,
	}, nil
}

func (s *S3Storage) listPrefix(ctx context.Context, hash common.Hash) []minio.ObjectInfo {
	results := make([]minio.ObjectInfo, 0)
	for object := range s.s3.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: path.Join(s.prefix, hash.String()), Recursive: true}) {
		results = append(results, object)
	}
	if len(results) > 0 {
		slices.SortFunc(results, func(a, b minio.ObjectInfo) int {
			return cmp.Compare(a.Key, b.Key)
		})
	}
	return results
}

func (s *S3Storage) Exists(ctx context.Context, hash common.Hash) (bool, error) {
	_, err := s.s3.StatObject(ctx, s.bucket, hash.String(), minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

type fetchedBlob struct {
	Index       int
	BlobSidecar *deneb.BlobSidecar
}

func (s *S3Storage) Read(ctx context.Context, hash common.Hash) (BlobData, error) {
	objInfos := s.listPrefix(ctx, hash)
	if len(objInfos) == 0 {
		s.log.Info("unable to find blob", "hash", hash.String())
		return BlobData{}, ErrNotFound
	}

	blobResultsCh := make(chan fetchedBlob, len(objInfos))
	eg, egCtx := errgroup.WithContext(ctx)

	for i, objInfo := range objInfos {
		i, objInfo := i, objInfo
		eg.Go(func() error {
			// s.log.Info("fetching blob", "i", i, "key", objInfo.Key)

			res, err := s.s3.GetObject(egCtx, s.bucket, objInfo.Key, minio.GetObjectOptions{})
			if err != nil {
				s.log.Warn("unexpected error fetching blob", "key", objInfo.Key, "err", err)
				return ErrStorage
			}
			defer res.Close()

			var reader io.ReadCloser = res
			defer reader.Close()

			if objInfo.Metadata.Get("Content-Encoding") == "gzip" {
				reader, err = gzip.NewReader(reader)
				if err != nil {
					s.log.Warn("error creating gzip reader", "key", objInfo.Key, "err", err)
					return ErrMarshaling
				}
			}

			var blobRecvBuf bytes.Buffer
			blobRecvBuf.ReadFrom(reader)

			data := new(deneb.BlobSidecar)

			err = data.UnmarshalSSZ(blobRecvBuf.Bytes())
			if err != nil {
				s.log.Warn("error decoding blob", "key", objInfo.Key, "err", err)
				return ErrMarshaling
			}

			// s.log.Info("got blob ph1", "i", i, "index", data.Index, "data", data.Blob[:12])
			blobResultsCh <- fetchedBlob{i, data}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return BlobData{}, err
	}

	close(blobResultsCh)

	blobs := make([]*deneb.BlobSidecar, len(objInfos))
	for aFetchedBlob := range blobResultsCh {
		blobs[aFetchedBlob.Index] = aFetchedBlob.BlobSidecar
	}

	// s.log.Info("fetched blobs", "count", len(blobs))

	// for i, blob := range blobs {
	//	s.log.Info("got blob ph2", "i", i, "index", blob.Index, "data", blob.Blob[:12])
	// }

	return BlobData{
		Header:       Header{BeaconBlockHash: hash},
		BlobSidecars: BlobSidecars{Data: blobs},
	}, nil
}

func (s *S3Storage) Write(ctx context.Context, data BlobData) error {
	b, err := json.Marshal(data)
	if err != nil {
		s.log.Warn("error encoding blob", "err", err)
		return ErrMarshaling
	}

	options := minio.PutObjectOptions{
		ContentType: "application/json",
	}

	if s.compress {
		b, err = compress(b)
		if err != nil {
			s.log.Warn("error compressing blob", "err", err)
			return ErrCompress
		}
		options.ContentEncoding = "gzip"
	}

	reader := bytes.NewReader(b)

	_, err = s.s3.PutObject(ctx, s.bucket, data.Header.BeaconBlockHash.String(), reader, int64(len(b)), options)

	if err != nil {
		s.log.Warn("error writing blob", "err", err)
		return ErrStorage
	}

	s.log.Info("wrote blob", "hash", data.Header.BeaconBlockHash.String())
	return nil
}

func compress(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(in)
	if err != nil {
		return nil, err
	}
	err = gz.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
