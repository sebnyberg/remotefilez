package remotefilez_test

import (
	"io"
	"os"
	"testing"

	"github.com/sebnyberg/remotefilez"
	"github.com/stretchr/testify/require"
)

func TestValidation(t *testing.T) {
	t.Run("unknown scheme", func(t *testing.T) {
		var p remotefilez.Opener
		_, err := p.Open("abc123://")
		require.ErrorIs(t, err, remotefilez.ErrUnsupportedScheme)
	})
	t.Run("relative path errs", func(t *testing.T) {
		fpath := "./testdata/small"
		furi := "file://" + fpath
		var p remotefilez.Opener
		_, err := p.Open(furi)
		require.ErrorIs(t, err, remotefilez.ErrRelativePath)
	})

}

func TestLocal(t *testing.T) {
	dir, err := os.Getwd()
	require.NoError(t, err)
	fpath := dir + "/testdata/small"
	furi := "file://" + fpath
	t.Run(fpath, func(t *testing.T) {
		// Read with reqular os.Open
		wantf, err := os.Open(fpath)
		require.NoError(t, err)
		want, err := io.ReadAll(wantf)
		require.NoError(t, err)

		// Read with remotefilez opener
		var p remotefilez.Opener
		f, err := p.Open(furi)
		require.NoError(t, err)
		actual, err := io.ReadAll(f)
		require.NoError(t, err)
		require.Equal(t, want, actual)
	})

}
