package miniofs

import (
	"context"
	"errors"
	"os"

	"github.com/minio/minio-go/v7"
)

func mapMinioError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ErrContextCanceled
	}

	response := minio.ToErrorResponse(err)
	switch response.Code {
	case "NoSuchKey", "NoSuchObject", "NotFound", "NoSuchVersion":
		return os.ErrNotExist
	}

	return err
}

func errorsIsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}
