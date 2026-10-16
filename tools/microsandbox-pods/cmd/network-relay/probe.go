package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/floci-io/floci/tools/microsandbox-pods/internal/healthprobe"
)

// This command runs through the controller's Docker exec authority, not a public listener.
func probe(input io.Reader, output io.Writer) error {
	decoder := json.NewDecoder(io.LimitReader(input, 65537))
	decoder.DisallowUnknownFields()
	var request healthprobe.Request
	if err := decoder.Decode(&request); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing probe data")
	}
	result, err := healthprobe.Check(context.Background(), request)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}
