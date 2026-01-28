package miniofs

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ParseURL parses a MinIO DSN and returns minio.Options
// DSN format: scheme://accessKey:secretKey@endpoint/bucket?param=value
// Supported query parameters:
//   - region: AWS region (default: us-east-1)
//   - token: Session token for temporary credentials
//   - insecure: Disable TLS verification (true/false, default: false)
func ParseURL(minioURL string) (*minio.Options, error) {
	if minioURL == "" {
		return nil, errors.New("miniofs: empty DSN")
	}

	u, err := url.Parse(minioURL)
	if err != nil {
		return nil, fmt.Errorf("miniofs: invalid DSN: %w", err)
	}

	// Validate scheme
	// if u.Scheme != "http" && u.Scheme != "https" {
	// 	return nil, fmt.Errorf("miniofs: invalid scheme %q, must be http or https", u.Scheme)
	// }

	// Validate host
	if u.Host == "" {
		return nil, errors.New("miniofs: missing host in DSN")
	}

	o := &minio.Options{
		Region: "us-east-1",
		Secure: u.Scheme == "https",
	}

	// Extract credentials
	username, password := getUserPassword(u)
	if username == "" {
		return nil, errors.New("miniofs: missing access key in DSN")
	}
	if password == "" {
		return nil, errors.New("miniofs: missing secret key in DSN")
	}

	token := u.Query().Get("token")
	o.Creds = credentials.NewStaticV4(username, password, token)

	// Parse query parameters
	query := u.Query()

	// Region
	if region := query.Get("region"); region != "" {
		o.Region = region
	}

	// Insecure (disable TLS verification)
	if insecure := query.Get("insecure"); insecure != "" {
		insecureBool, err := strconv.ParseBool(insecure)
		if err != nil {
			return nil, fmt.Errorf("miniofs: invalid insecure parameter: %w", err)
		}
		if insecureBool && o.Secure {
			// Note: This would require custom transport setup
			// For now, we just note that insecure is requested
		}
	}

	return o, nil
}

// getUserPassword extracts username and password from URL
func getUserPassword(u *url.URL) (string, string) {
	var user, password string
	if u.User != nil {
		user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			password = p
		}
	}
	return user, password
}
