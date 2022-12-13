package remotefilez_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/sebnyberg/remotefilez"
	"github.com/stretchr/testify/require"
)

// This file is on my private account. For testing you'll have to upload your
// own version.
const (
	azPublicBucket    = "saipubbucketestingonly"
	azPublicContainer = "testcontainer"
	azPublicBlob      = "small.csv"
)

func TestAzure(t *testing.T) {
	creds, err := azidentity.NewDefaultAzureCredential(nil)
	require.NoError(t, err)
	serviceURL := fmt.Sprintf("https://%v.blob.core.windows.net", azPublicBucket)
	client, err := azblob.NewClient(serviceURL, creds, nil)
	require.NoError(t, err)

	// Dump contents to file and seek to start
	bg := context.Background()
	f1, err := os.CreateTemp("", "")
	require.NoError(t, err)
	n, err := client.DownloadFile(bg, azPublicContainer, azPublicBlob, f1, nil)
	require.NoError(t, err)
	_, err = f1.Seek(0, os.SEEK_SET)
	require.NoError(t, err)

	// Open remotefilez file
	var ro remotefilez.Opener
	blobURL := fmt.Sprintf("abs://%v.blob.core.windows.net/%v/%v",
		azPublicBucket, azPublicContainer, azPublicBlob,
	)
	_, err = ro.Open(blobURL)
	require.Error(t, err)
	ro = *ro.WithAzureResolver(creds, 0)
	f2, err := ro.Open(blobURL)
	require.NoError(t, err)

	// Try to read a bunch of different offsets and data from the file
	whencestr := []string{"SEEK_CUR", "SEEK_SET", "SEEK_END"}
	whences := []int{
		io.SeekStart,
		io.SeekCurrent,
		io.SeekEnd,
	}
	for _, whence := range whences {
		// Reset
		buf1 := make([]byte, n)
		buf2 := make([]byte, n)

		for bufsz := int64(10); bufsz <= n; bufsz *= 10 {
			buf1 = buf1[:bufsz]
			buf2 = buf2[:bufsz]

			for off := int64(0); off <= n; off = (off + 3) * 7 {
				tname := fmt.Sprintf("whence=%v,bufsz=%v,off=%v",
					whencestr[whence], bufsz, off,
				)
				t.Run(tname, func(t *testing.T) {
					// Seek to offset
					off1, err1 := f1.Seek(off, whence)
					off2, err2 := f2.Seek(off, whence)
					require.Equal(t, err1, err2, "errs must match")
					require.Equal(t, off1, off2, "start offset must match")

					// Read until error
					for {
						m1, err1 := f1.Read(buf1)
						m2, err2 := f2.Read(buf2)
						require.True(t, bytes.Equal(buf1, buf2), "bufs must match")
						require.Equal(t, m1, m2, "length must match")
						if err1 != err2 {
							t.Fatalf("err1 != err2")
						}
						require.Equal(t, err1, err2, "errs must match")
						if err1 != nil {
							require.ErrorIs(t, err1, io.EOF)
							break
						}
					}
				})
			}
		}
	}
}
