# remotefilez

Sensible defaults handles to various (local and Azure) file URLs as `io.ReadSeekCloser` and `io.WriteCloser`.

`io.WriteSeekCloser` is not supported at the moment.

This package is experimental. Do not use it for production workloads.

Below are some (limited) examples. Please handle errors, .Close() and timeouts
properly. 

## Local file

```go
var ro remotefilez.Opener

localfile, err := ro.OpenReader("file:///path/to/local/file.txt")
if err != nil { ... }

defer localfile.Close()
contents, err := ioutil.ReadAll(localfile)
if err != nil { ... }
```

## Azure Blob Storage

```go
var ro remotefilez.Opener

creds, err := azidentity.NewDefaultAzureCredentials(nil)
if err != nil { ... }
readTimeout := time.Second*1
ro = ro.WithAzure(creds, readTimeout)

storageAcc := "mystorageacct"
containerName := "mycontainer"
blobPath := "path/to/blob.txt"
blobURL := fmt.Sprintf(
    "abs://%v.blob.core.windows.net/%v/%v",
    storageAcc, containerName, blobPath,
)

bgCtx := context.Background()
ctx, cancel := context.WithTimeout(bgCtx, time.Second*10)
defer cancel()

// Upload ./somefile.txt to path/to/blob.txt
f, err := os.Open("somefile.txt")
if err != nil { ... }

azwrite, err := ro.OpenWriterCtx(blobURL, ctx)
if err != nil { ... }

// Copy
_, err = io.Copy(azwrite, f)
if err != nil { ... }
err = azwrite.Close() // IMPORTANT!
if err != nil { ... }

// Read blob contents
azread, err := ro.OpenReaderCtx(blobURL, ctx)
if err != nil { ... }

contents, err := ioutil.ReadAll(azread)
if err != nil { ... }
err = azread.Close()
if err != nil { ... }
```
