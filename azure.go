package remotefilez

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"golang.org/x/exp/constraints"
)

var (
	ErrInvalidBlobURL = errors.New("invalid blob url")
)

type azSeekCloser struct {
	blob        *blob.Client
	resp        *blob.DownloadStreamResponse
	mtx         sync.Mutex
	n           int64
	off         int64
	err         error
	readTimeout time.Duration
}

func NewAzureBlobReadSeekCloser(
	url string,
	creds azcore.TokenCredential,
	readTimeout time.Duration,
	openCtx context.Context,
) (io.ReadSeekCloser, error) {
	if creds == nil {
		return nil, errors.New("nil credentials")
	}
	bucketName, containerName, blobName := parseAzBlobName(url)
	if bucketName == "" {
		return nil, fmt.Errorf("%w, missing bucket name", ErrInvalidBlobURL)
	}
	if containerName == "" {
		return nil, fmt.Errorf("%w, missing container name", ErrInvalidBlobURL)
	}
	if blobName == "" {
		return nil, fmt.Errorf("%w, missing blob name", ErrInvalidBlobURL)
	}

	// Initialize client
	blobURL := fmt.Sprintf("https://%v.blob.core.windows.net/%v/%v",
		bucketName, containerName, blobName)
	var clientOpts blob.ClientOptions
	clientOpts.Retry.TryTimeout = readTimeout
	blobClient, err := blob.NewClient(blobURL, creds, &clientOpts)
	if err != nil {
		return nil, err
	}

	// Retrieve blob size
	resp, err := blobClient.GetProperties(openCtx, nil)
	if err != nil {
		return nil, err
	}
	if resp.ContentLength == nil || *resp.ContentLength == 0 {
		return nil, errors.New("unexpected: nil blob length")
	}

	// Init
	var sc azSeekCloser
	sc.n = *resp.ContentLength
	sc.blob = blobClient
	sc.readTimeout = readTimeout
	if _, err := sc.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}

	return &sc, nil
}

// Read reads up to len(p) bytes into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered. Even if Read
// returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, Read conventionally
// returns what is available instead of waiting for more.
//
// When Read encounters an error or end-of-file condition after
// successfully reading n > 0 bytes, it returns the number of
// bytes read. It may return the (non-nil) error from the same call
// or return the error (and n == 0) from a subsequent call.
// An instance of this general case is that a Reader returning
// a non-zero number of bytes at the end of the input stream may
// return either err == EOF or err == nil. The next Read should
// return 0, EOF.
//
// Callers should always process the n > 0 bytes returned before
// considering the error err. Doing so correctly handles I/O errors
// that happen after reading some bytes and also both of the
// allowed EOF behaviors.
//
// Implementations of Read are discouraged from returning a
// zero byte count with a nil error, except when len(p) == 0.
// Callers should treat a return of 0 and nil as indicating that
// nothing happened; in particular it does not indicate EOF.
//
// Implementations must not retain p.
func (sc *azSeekCloser) Read(p []byte) (n int, err error) {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	return sc.read(p)
}

// read is a concurrency-unsafe version of .Read(). You must hold sc.mtx before
// calling this function.
func (sc *azSeekCloser) read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if sc.off >= sc.n {
		return 0, io.EOF
	}
	if sc.err != nil {
		return 0, sc.err
	}
	if sc.resp == nil {
		return 0, errors.New("unexpected: nil resp")
	}
	n, err = sc.resp.Body.Read(p)
	sc.off += int64(n)
	if err == io.EOF {
		r := sc.resp
		sc.resp = nil
		if err := r.Body.Close(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// Seek sets the offset for the next Read or Write to offset,
// interpreted according to whence:
// SeekStart means relative to the start of the file,
// SeekCurrent means relative to the current offset, and
// SeekEnd means relative to the end
// (for example, offset = -2 specifies the penultimate byte of the file).
// Seek returns the new offset relative to the start of the
// file or an error, if any.
//
// Seeking to an offset before the start of the file is an error.
// Seeking to any positive offset may be allowed, but if the new offset exceeds
// the size of the underlying object the behavior of subsequent I/O operations
// is implementation-dependent.
func (sc *azSeekCloser) Seek(offset int64, whence int) (int64, error) {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	return sc.seek(offset, whence)
}

// seek is a concurrency-unsafe version of .Seek(). You must hold sc.mtx before
// calling this function.
func (sc *azSeekCloser) seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:

		if err := sc.close(); err != nil {
			return 0, err
		}

		if offset < 0 {
			return 0, errors.New("offset out of bounds")
		}
		sc.off = offset
		if sc.off >= sc.n {
			return sc.off, nil
		}

		var o blob.DownloadStreamOptions
		o.Range.Count = sc.n - sc.off
		o.Range.Offset = sc.off

		resp, err := sc.blob.DownloadStream(context.Background(), &o)
		sc.resp = &resp
		sc.err = err

		return sc.off, sc.err

	case io.SeekEnd:
		if err := sc.close(); err != nil {
			return 0, err
		}

		if sc.n+offset < 0 {
			return 0, errors.New("offset out of bounds")
		}
		if sc.off+offset > sc.n {
			return offset + sc.n, nil
		}
		sc.off += offset
		if sc.off == sc.n {
			return sc.off, nil
		}

		var o blob.DownloadStreamOptions
		o.Range.Count = sc.n - sc.off
		o.Range.Offset = sc.off

		resp, err := sc.blob.DownloadStream(context.Background(), &o)
		sc.resp = &resp
		sc.err = err

		return sc.off, sc.err

	case io.SeekCurrent:
		if offset >= 0 {
			if sc.resp != nil {
				// FFWD
				gap := min(offset, sc.n-sc.off)
				m, err := io.CopyN(io.Discard, sc.resp.Body, gap)
				sc.off += m
				return sc.off, err
			}
			return sc.seek(sc.off+offset, io.SeekStart)
		}
		return sc.seek(sc.off+offset, io.SeekStart)
	}

	return 0, errors.New("invalid whence")
}

// Close closes the underlying blob connection.
func (sc *azSeekCloser) Close() error {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	return sc.close()
}

// close is a concurrency-unsafe version of .Close(). You must hold sc.mtx before
// calling this function.
func (sc *azSeekCloser) close() error {
	if sc.resp != nil {
		r := sc.resp
		sc.resp = nil
		sc.off = 0
		return r.Body.Close()
	}
	return nil
}

var blobPattern = regexp.MustCompile(`(https|abs)://([^/\.]+)(\.blob\.core\.windows\.net)/(.*)/(.*)`)

func parseAzBlobName(url string) (storageAccount, container, blob string) {
	i := blobPattern.FindStringSubmatchIndex(url)
	if i[2] == -1 {
		return "", "", ""
	}
	if i[6] == -1 {
		return "", "", ""
	}
	if i[8] == -1 {
		return "", "", ""
	}
	return url[i[4]:i[5]], url[i[8]:i[9]], url[i[10]:i[11]]
}

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}
