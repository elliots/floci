module github.com/floci-io/floci/tools/microsandbox-host

go 1.26.0

require github.com/floci-io/floci/tools/microsandbox-runtime v0.0.0

require github.com/superradcompany/microsandbox/sdk/go v0.7.8 // indirect

replace github.com/floci-io/floci/tools/microsandbox-runtime => ../microsandbox-runtime
