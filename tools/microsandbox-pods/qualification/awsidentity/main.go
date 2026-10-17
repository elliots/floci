// The same AWS SDK probe runs in ordinary and native Kubernetes Pods.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

func main() {
	ctx := context.Background()
	options := []func(*config.LoadOptions) error{config.WithRegion("us-east-1"), config.WithRetryMaxAttempts(1)}
	if text := os.Getenv("QUALIFICATION_EXPIRY_WINDOW_SECONDS"); text != "" {
		seconds, err := strconv.Atoi(text)
		if err != nil || seconds < 0 || seconds > 3600 {
			log.Fatal("invalid qualification credential cache window")
		}
		options = append(options, config.WithCredentialsCacheOptions(func(value *aws.CredentialsCacheOptions) {
			value.ExpiryWindow = time.Duration(seconds) * time.Second
		}))
	}
	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		log.Fatal("AWS SDK configuration failed")
	}
	if len(os.Args) == 2 && os.Args[1] == "--credentials-once" {
		credentials, err := cfg.Credentials.Retrieve(ctx)
		if err != nil {
			log.Fatal("AWS SDK credential retrieval failed")
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"source": credentials.Source})
		return
	}
	probe := &identityProbe{cfg: cfg, identity: sts.NewFromConfig(cfg), parameters: ssm.NewFromConfig(cfg), marker: rand.Text()}
	server := &http.Server{Addr: ":8080", Handler: http.HandlerFunc(probe.serve), ReadHeaderTimeout: 10 * time.Second}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("qualification listener failed")
	}
}

type identityProbe struct {
	cfg        aws.Config
	identity   *sts.Client
	parameters *ssm.Client
	marker     string
}

func code(err error) string {
	if err == nil {
		return ""
	}
	if api, ok := errors.AsType[smithy.APIError](err); ok {
		if value := api.ErrorCode(); value != "" {
			return value
		}
	}
	if response, ok := errors.AsType[interface {
		error
		HTTPStatusCode() int
	}](err); ok {
		return fmt.Sprintf("EndpointHTTP%d", response.HTTPStatusCode())
	}
	return "TransportOrCredentialsError"
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func (p *identityProbe) serve(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result := map[string]any{"marker": p.marker}
	credentials, err := p.cfg.Credentials.Retrieve(ctx)
	if err != nil {
		result["error"] = code(err)
	} else {
		result["source"] = credentials.Source
		result["credentialHash"] = digest([]byte(credentials.AccessKeyID))
		result["expires"] = credentials.Expires
		for _, variable := range []string{"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"} {
			if path := os.Getenv(variable); path != "" {
				if value, err := os.ReadFile(path); err == nil {
					result["tokenHash"] = digest(value)
					parts := strings.Split(strings.TrimSpace(string(value)), ".")
					if len(parts) == 3 {
						payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
						var claims struct {
							Expires int64 `json:"exp"`
						}
						if json.Unmarshal(payload, &claims) == nil {
							result["tokenExpires"] = claims.Expires
						}
					}
				}
			}
		}
		caller, callerErr := p.identity.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if callerErr != nil {
			result["callerError"] = code(callerErr)
		} else {
			result["callerArn"] = aws.ToString(caller.Arn)
		}
		allowed, allowedErr := p.parameters.GetParameter(ctx, &ssm.GetParameterInput{Name: new(os.Getenv("QUALIFICATION_ALLOWED_PARAMETER"))})
		if allowedErr != nil {
			result["allowedError"] = code(allowedErr)
		} else {
			result["allowed"] = aws.ToString(allowed.Parameter.Value)
		}
		_, deniedErr := p.parameters.GetParameter(ctx, &ssm.GetParameterInput{Name: new(os.Getenv("QUALIFICATION_DENIED_PARAMETER"))})
		result["denied"] = code(deniedErr)
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "qualification response disconnected")
	}
}
