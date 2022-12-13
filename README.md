# remotefilez

Open handles to various (local and Azure) file URLs as an `io.ReadSeekCloser`.

This package is experimental. Do not use it for production workloads.

Below are some (limited) examples. Please handle errors, .Close() and timeouts
properly. 

## Local file

```go
var ro remotefilez.Opener

localfile, err := ro.Open("file:///path/to/local/file.txt")
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
openCtx, cancel := context.WithTimeout(bgCtx, time.Second*10)
defer cancel()
azfile, err := ro.OpenCtx(blobURL, ctx)
if err != nil { ... }

defer azfile.Close()
contents, err := ioutil.ReadAll(azfile)
if err != nil { ... }
```
