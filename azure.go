package remotefilez

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"golang.org/x/exp/constraints"
)

// Interface guards
var _ io.Closer = (*azReader)(nil)
var _ io.Reader = (*azReader)(nil)
var _ io.ReaderAt = (*azReader)(nil)
var _ interface{ Size() (int64, error) } = (*azReader)(nil)
var _ io.Closer = (*azWriter)(nil)
var _ io.Writer = (*azWriter)(nil)

var (
	ErrInvalidBlobURL = errors.New("invalid blob url")
)

type accounting struct {
	readSz    [32]uint32
	readCount uint32

	readAtFastpath uint32
	readAtSlowpath uint32
}

type azReader struct {
	blob *blob.Client
	resp *blob.DownloadStreamResponse
	mtx  sync.Mutex
	n    int64
	off  int64
	err  error
	ctx  context.Context

	doAcct bool
	acct   accounting
}

func NewAzureBlobReader(
	ctx context.Context,
	blobURL string,
	creds azcore.TokenCredential,
	openTimeout time.Duration,
	doAcct bool,
) (*azReader, error) {
	if creds == nil {
		return nil, errors.New("nil credentials")
	}
	u, err := url.Parse(blobURL)
	if err != nil {
		return nil, ErrInvalidBlobURL
	}
	u.Scheme = "https"

	// Initialize client
	blobClient, err := blob.NewClient(u.String(), creds, nil)
	if err != nil {
		return nil, err
	}

	// Retrieve blob size
	resp, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, err
	}
	if resp.ContentLength == nil || *resp.ContentLength == 0 {
		return nil, errors.New("unexpected: nil blob length")
	}

	// Init
	var sc azReader
	sc.n = *resp.ContentLength
	sc.blob = blobClient
	sc.ctx = ctx
	if _, err := sc.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	if doAcct {
		sc.doAcct = true
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
func (sc *azReader) Read(p []byte) (n int, err error) {

	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	return sc.read(p)
}

func (sc *azReader) Size() (int64, error) {
	return sc.n, nil
}

// read is a concurrency-unsafe version of .Read(). You must hold sc.mtx before
// calling this function.
func (sc *azReader) read(p []byte) (n int, err error) {
	sc.accountRead(p)

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

func (sc *azReader) accountRead(p []byte) {
	if !sc.doAcct {
		return
	}
	sz := len(p)
	for i := 31; i >= 0; i-- {
		if sz&(1<<i) > 0 {
			atomic.AddUint32(&sc.acct.readSz[i], 1)
			break
		}
	}
	atomic.AddUint32(&sc.acct.readCount, 1)
	if atomic.LoadUint32(&sc.acct.readCount)%10000 == 0 {
		fmt.Printf("Read distribution\n")
		for i := 31; i >= 0; i-- {
			fmt.Printf("Sz: %v, Count: %v\n", 1<<i, atomic.LoadUint32(&sc.acct.readSz[i]))
		}
	}
}

func (sc *azReader) ReadAt(p []byte, off int64) (n int, err error) {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	if off == sc.off {
		// fastpath
		sc.acct.readAtFastpath++
		return sc.read(p)
	} else {
		sc.acct.readAtSlowpath++
	}
	if _, err := sc.seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return sc.read(p)
}

func (sc *azReader) readAtAccount(p []byte, off int64) {
	if !sc.doAcct {
		return
	}
	if off == sc.off {
		sc.acct.readAtFastpath++
	} else {
		sc.acct.readAtSlowpath++
	}
	if (sc.acct.readAtFastpath+sc.acct.readAtSlowpath)%100000 == 0 {
		fmt.Printf("ReadAt distribution\n")
		fmt.Printf("Fastpath: %v\n", sc.acct.readAtFastpath)
		fmt.Printf("Slowpath: %v\n", sc.acct.readAtSlowpath)
	}
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
func (sc *azReader) Seek(offset int64, whence int) (int64, error) {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	return sc.seek(offset, whence)
}

// seek is a concurrency-unsafe version of .Seek(). You must hold sc.mtx before
// calling this function.
func (sc *azReader) seek(offset int64, whence int) (int64, error) {
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

		resp, err := sc.blob.DownloadStream(sc.ctx, &o)
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

		resp, err := sc.blob.DownloadStream(sc.ctx, &o)
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
func (sc *azReader) Close() error {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	return sc.close()
}

// close is a concurrency-unsafe version of .Close(). You must hold sc.mtx before
// calling this function.
func (sc *azReader) close() error {
	if sc.resp != nil {
		r := sc.resp
		sc.resp = nil
		sc.off = 0
		return r.Body.Close()
	}
	return nil
}

type azWriter struct {
	blob *blockblob.Client
	mtx  sync.Mutex
	n    int64
	err  error
	w    io.WriteCloser
}

// NewAzureBlobWriteCloser returns an io.WriteCloser that can be used to write
// to an Azure Blob.
func NewAzureBlobWriteCloser(
	blobURL string,
	creds azcore.TokenCredential,
	openTimeout time.Duration,
	ctx context.Context,
) (*azWriter, error) {
	if creds == nil {
		return nil, errors.New("nil credentials")
	}
	u, err := url.Parse(blobURL)
	if err != nil {
		return nil, ErrInvalidBlobURL
	}
	u.Scheme = "https"

	// Initialize client
	blobClient, err := blockblob.NewClient(u.String(), creds, nil)
	if err != nil {
		return nil, err
	}

	// Init
	var sc azWriter
	sc.blob = blobClient

	r, w := io.Pipe()
	go func() {
		_, err := blobClient.UploadStream(ctx, r, nil)
		if err != nil {
			sc.mtx.Lock()
			defer sc.mtx.Unlock()
			sc.err = err
			return
		}
	}()
	sc.w = w

	return &sc, nil
}

// Write implements io.Writer
func (sc *azWriter) Write(p []byte) (n int, err error) {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	return sc.w.Write(p)
}

// Close closes the underlying blob connection.
func (sc *azWriter) Close() error {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()
	closeErr := sc.w.Close()
	if sc.err != nil {
		return sc.err
	}
	return closeErr
}

var blobPattern = regexp.MustCompile(`(https|abs)://([^/\.]+)(\.blob\.core\.windows\.net)/(.*)/(.*)`)

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}
