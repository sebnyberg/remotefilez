package remotefilez

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

const (
	schemeFile  = "file"
	schemeAzure = "abs"
)

var (
	ErrRelativePath      = errors.New("relative path")
	ErrUnsupportedScheme = errors.New("unsupported scheme")
	ErrNotImplemented    = errors.New("not implemented")
)

// Opener provides a unified interface for resolving io.ReadSeekClosers from
// URLs.
type Opener struct {
	azcreds  azcore.TokenCredential
	aztimout time.Duration
}

// WithAzureResolver returns a copy of the Opener with the provided Azure
// Resolver.
func (ro Opener) WithAzureResolver(
	creds azcore.TokenCredential,
	timeout time.Duration,
) *Opener {
	ro.azcreds = creds
	ro.aztimout = timeout
	return &ro
}

// Open returns an io.ReadSeekCloser handle from the provided file URL.
//
// Depecated: Use OpenReader instead.
func (ro *Opener) Open(fileURL string) (io.ReadSeekCloser, error) {
	return ro.OpenReader(fileURL)
}

// OpenCtx returns an io.ReadSeekCloser handle from the provided file URL.
// Errors if a resolver for the provided schema is not registered.
//
// Depecated: Use OpenReaderCtx instead.
func (ro *Opener) OpenCtx(ctx context.Context, fileURL string) (io.ReadSeekCloser, error) {
	return ro.OpenReaderCtx(ctx, fileURL)
}

// OpenReader returns an io.ReadSeekCloser handle from the provided file URL.
func (ro *Opener) OpenReader(fileURL string) (io.ReadSeekCloser, error) {
	ctx := context.Background()
	return ro.OpenCtx(ctx, fileURL)
}

// OpenReaderCtx returns an io.ReadSeekCloser handle from the provided file URL.
// Errors if a resolver for the provided schema is not registered.
func (ro *Opener) OpenReaderCtx(ctx context.Context, fileURL string) (io.ReadSeekCloser, error) {
	u, err := url.Parse(fileURL)
	if err != nil {
		return nil, fmt.Errorf("parse URL failed, %w", err)
	}

	// Best-effort to detect absolute paths
	if len(fileURL) >= len(u.Scheme)+4 && fileURL[len(u.Scheme)+3] == '.' {
		return nil, fmt.Errorf("%w not supported", ErrRelativePath)
	}

	switch u.Scheme {
	case schemeFile:
		return os.OpenFile(u.Path, os.O_RDWR, 0)
	case schemeAzure:
		if ro.azcreds == nil {
			return nil, errors.New("missing credentials please add AzureResolver")
		}
		return NewAzureBlobReadSeekCloser(fileURL, ro.azcreds, ro.aztimout, ctx)
	default:
		return nil, fmt.Errorf("%w %q", ErrUnsupportedScheme, u.Scheme)
	}
}

// Open returns an io.ReadSeekCloser handle from the provided file URL.
func (ro *Opener) OpenWriter(fileURL string) (io.WriteCloser, error) {
	ctx := context.Background()
	return ro.OpenWriterCtx(ctx, fileURL)
}

// OpenCtx returns an io.ReadSeekCloser handle from the provided file URL.
// Errors if a resolver for the provided schema is not registered.
func (ro *Opener) OpenWriterCtx(ctx context.Context, fileURL string) (io.WriteCloser, error) {
	u, err := url.Parse(fileURL)
	if err != nil {
		return nil, fmt.Errorf("parse URL failed, %w", err)
	}

	// Best-effort to detect absolute paths
	if len(fileURL) >= len(u.Scheme)+4 && fileURL[len(u.Scheme)+3] == '.' {
		return nil, fmt.Errorf("%w not supported", ErrRelativePath)
	}

	switch u.Scheme {
	case schemeFile:
		return os.OpenFile(u.Path, os.O_RDWR, 0)
	case schemeAzure:
		if ro.azcreds == nil {
			return nil, errors.New("missing credentials please add AzureResolver")
		}
		return NewAzureBlobWriteCloser(fileURL, ro.azcreds, ro.aztimout, ctx)
	default:
		return nil, fmt.Errorf("%w %q", ErrUnsupportedScheme, u.Scheme)
	}
}
