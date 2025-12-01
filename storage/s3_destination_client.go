package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/domain"
)

type S3DestinationClient struct {
	client      *s3.Client
	destination *domain.S3Destination
}

func NewS3DestinationClient(destination *domain.S3Destination) (*S3DestinationClient, error) {
	if destination == nil {
		return nil, fmt.Errorf("S3 destination is nil")
	}

	if destination.BucketName == "" {
		return nil, fmt.Errorf("S3 destination bucket name is required")
	}

	if destination.Region == "" {
		destination.Region = "us-east-1" // Default region
	}

	// Create AWS config
	var awsCfg aws.Config
	var err error

	// Determine credential source based on UseIamRole flag:
	// - If UseIamRole is true: use default credential chain (IAM role, instance profile, etc.)
	// - If UseIamRole is false: use static credentials (AccessKey and SecretKey)
	if destination.UseIamRole {
		// Use default credential chain (IAM role, environment variables, etc.)
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(destination.Region),
		)
	} else {
		// Use static credentials (access key + secret key)
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(destination.Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				destination.AccessKey,
				destination.SecretKey,
				"",
			)),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Configure custom endpoint if provided (for MinIO, etc.)
	var s3Client *s3.Client
	if destination.Endpoint != "" {
		// Custom endpoint configuration
		endpoint := destination.Endpoint
		if !strings.HasPrefix(endpoint, "http") {
			if destination.Ssl {
				endpoint = "https://" + endpoint
			} else {
				endpoint = "http://" + endpoint
			}
		}

		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // Required for MinIO and some custom S3 implementations
		})
	} else {
		// Standard AWS S3
		s3Client = s3.NewFromConfig(awsCfg)
	}

	return &S3DestinationClient{
		client:      s3Client,
		destination: destination,
	}, nil
}

// PutObject uploads data to S3
func (c *S3DestinationClient) PutObject(key string, body io.Reader, contentType string) error {
	return c.PutObjectWithMetadata(key, body, contentType, nil)
}

// PutObjectWithMetadata uploads data to S3 with custom metadata
func (c *S3DestinationClient) PutObjectWithMetadata(key string, body io.Reader, contentType string, metadata map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Add prefix if configured
	s3Key := key
	if c.destination.Prefix != "" {
		s3Key = c.destination.Prefix + key
	}

	log.Debugf("Uploading to S3: bucket=%s, key=%s, metadata=%v", c.destination.BucketName, s3Key, metadata)

	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.destination.BucketName),
		Key:         aws.String(s3Key),
		Body:        body,
		ContentType: aws.String(contentType),
		Metadata:    metadata, // AWS SDK v2 uses map[string]string directly
	})

	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	log.Debugf("Successfully uploaded to S3: bucket=%s, key=%s", c.destination.BucketName, s3Key)
	return nil
}

// TestConnection tests the S3 connection
func (c *S3DestinationClient) TestConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Debugf("Testing S3 destination connection to bucket: %s", c.destination.BucketName)

	// Try to head the bucket to verify access
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.destination.BucketName),
	})

	if err != nil {
		// Check if it's a NoSuchBucket error and try to create it
		if strings.Contains(err.Error(), "NoSuchBucket") || strings.Contains(err.Error(), "NotFound") {
			log.Warnf("Bucket %s does not exist, attempting to create it", c.destination.BucketName)

			// Try to create the bucket
			_, createErr := c.client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(c.destination.BucketName),
			})

			if createErr != nil {
				return fmt.Errorf("failed to create S3 destination bucket %s: %w",
					c.destination.BucketName, createErr)
			}

			log.Infof("Successfully created S3 destination bucket %s",
				c.destination.BucketName)
		} else {
			return fmt.Errorf("failed to access S3 destination bucket %s: %w",
				c.destination.BucketName, err)
		}
	}

	log.Infof("S3 destination connection successful to bucket: %s",
		c.destination.BucketName)
	return nil
}
