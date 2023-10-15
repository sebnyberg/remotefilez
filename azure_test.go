package remotefilez_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/ory/dockertest"
	"github.com/sebnyberg/remotefilez"
	"github.com/stretchr/testify/require"
)

// This file is on my private account. For testing you'll have to upload your
// own version.
const (
	azTestFile    = "beowulf.txt"
	azComposeMode = true
)

// get an Azure Storage container URL
func getContainerURL(tb testing.TB) *url.URL {
	if azComposeMode {
		return &url.URL{
			Scheme: "https",
			Path:   "dev/cnt",
			User:   url.UserPassword("dev", "c2VjcmV0a2V5Cg=="),
			Host:   "0.0.0.0:10000",
		}
	}
	pool, err := dockertest.NewPool("")
	require.NoError(tb, err)

	// Create a random names for the account, its key, and the container
	var rndBytes [24]byte
	_, err = io.ReadFull(rand.Reader, rndBytes[:])
	require.NoError(tb, err)
	accountName := hex.EncodeToString(rndBytes[:8])
	accountKey := hex.EncodeToString(rndBytes[8:16])
	containerName := hex.EncodeToString(rndBytes[16:])

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		Tag:        "latest",
		Env: []string{
			fmt.Sprintf("AZURITE_ACCOUNTS=%v:%v", accountName, accountKey),
		},
		Entrypoint: []string{},
		Cmd: []string{
			"azurite",
			"--oauth",
			"basic",
			"--blobHost",
			"0.0.0.0",
			"--cert",
			"/testdata/0.0.0.0.pem",
			"--key",
			"/testdata/0.0.0.0-key.pem",
		},
		Mounts: []string{
			fmt.Sprintf("%v/testdata:/testdata", os.Getenv("PWD")),
		},
	})
	require.NoError(tb, err)
	resource.Expire(60)
	tb.Cleanup(func() {
		require.NoError(tb, pool.Purge(resource))
	})

	containerURL := &url.URL{
		Scheme: "https",
		Path:   accountName,
		User:   url.UserPassword(accountName, accountKey),
		// Host:   net.JoinHostPort(resource.GetBoundIP("10000/tcp"), resource.GetPort("10000/tcp")),
		Host: net.JoinHostPort(resource.GetBoundIP("10000/tcp"), resource.GetPort("10000/tcp")),
	}

	// Todo(sn) logger
	// logWaiter, err := pool.Client.AttachToContainerNonBlocking(docker.AttachToContainerOptions{
	// 	Container:    resource.Container.ID,
	// 	OutputStream: os.Stdout,
	// 	ErrorStream:  os.Stderr,
	// 	Stdout:       true,
	// 	Stderr:       true,
	// 	Stream:       true,
	// })
	// require.NoError(tb, err)
	// tb.Cleanup(func() {
	// 	err = logWaiter.Close()
	// 	if err != nil {
	// 		tb.Fatalf("Could not close container log: %v", err)
	// 	}
	// 	err = logWaiter.Wait()
	// 	if err != nil {
	// 		tb.Fatalf("Could not wait for container log to close: %v", err)
	// 	}
	// })

	pool.MaxWait = 60 * time.Second
	err = pool.Retry(func() (err error) {
		creds, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return fmt.Errorf("creds err, %w", err)
		}
		storageClient, err := azblob.NewClient(containerURL.String(), creds, nil)
		if err != nil {
			return fmt.Errorf("new client err, %w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = storageClient.CreateContainer(ctx, containerName, nil)
		if err != nil {
			return fmt.Errorf("create container err, %w", err)
		}
		return err
	})
	require.NoError(tb, err)
	containerURL.Path = containerURL.Path + "/" + containerName
	return containerURL
}

func TestAzure(t *testing.T) {
	// This test verifies that there is no difference in reading and seeking
	// from a local file on disk than from a remote file on Azure.
	//
	// Azurite is used to test the Azure side of things.
	//
	creds, err := azidentity.NewDefaultAzureCredential(nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Open local test file
	testFilePath := "testdata/" + azTestFile
	f1, err := os.Open(testFilePath)
	require.NoError(t, err)

	// Record file size
	f1fi, err := os.Stat(testFilePath)
	require.NoError(t, err)
	n := f1fi.Size()

	// Get storage account connection
	containerURL := getContainerURL(t)
	blobURL := fmt.Sprintf("%v/%v", containerURL, azTestFile)

	// Open remotefilez file
	var ro remotefilez.Opener
	absURL, err := url.Parse(blobURL)
	require.NoError(t, err)
	absURL.Scheme = "abs"
	_, err = ro.Open(absURL.String())
	require.Error(t, err, "should require abs scheme")
	ro = *ro.WithAzureResolver(creds, 0)

	uploadOk := t.Run("upload", func(t *testing.T) {
		w, err := ro.OpenWriterCtx(ctx, absURL.String())
		require.NoError(t, err)
		n, err := io.Copy(w, f1)
		require.NoError(t, err)
		require.NoError(t, w.Close())
		require.Equal(t, n, f1fi.Size())
		f1.Seek(0, io.SeekStart)
	})
	require.True(t, uploadOk, "aborting test due to upload failure")

	// Open remotefilez file
	f2, err := ro.OpenReaderCtx(ctx, absURL.String())
	require.NoError(t, err)

	// Try to read a bunch of different offsets and data from the file
	whencestr := []string{"SeekStart", "SeekSet", "SeekEnd"}
	whences := []int{
		io.SeekStart,
		io.SeekCurrent,
		io.SeekEnd,
	}

	buf1 := bytes.NewBuffer(nil)
	buf2 := bytes.NewBuffer(nil)

	// Due to how Azure Blob Storage retrieves data, probably due to block
	// sizes, we cannot verify that lengths are the same with each read.
	// To solve this problem, let's use a two-pointer approach where we read
	// from the file from which the least amount of bytes have been read, then
	// compare the results byte-by-byte.
	for _, whence := range whences {
		for off := int64(0); off <= n; off = (off + 3) * 7 {
			tname := fmt.Sprintf("whence=%v,off=%v",
				whencestr[whence], off,
			)
			t.Run(tname, func(t *testing.T) {
				// Reset
				buf1.Reset()
				buf2.Reset()

				// Seek to offset
				off1, err1 := f1.Seek(off, whence)
				off2, err2 := f2.Seek(off, whence)
				require.Equal(t, err1, err2, "offset errs must match")
				require.Equal(t, off1, off2, "start offset must match")

				// Read until error
				m1, err1 := io.Copy(buf1, f1)
				m2, err2 := io.Copy(buf2, f2)
				if m1 != m2 {
					require.Equal(t, m1, m2, "length must match")
				}
				if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
					t.Fatalf("bufs must match")
				}
				require.Equal(t, err1, err2, "errs must match")
			})
		}
	}
}
