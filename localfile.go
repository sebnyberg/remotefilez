package remotefilez

import "os"

type sizedFile struct {
	*os.File
}

func (f *sizedFile) Size() (int64, error) {
	fi, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), err
}
