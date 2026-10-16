module github.com/floci-io/floci/tools/microsandbox-host

go 1.26.0

require github.com/floci-io/floci/tools/microsandbox-runtime v0.0.0

require (
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
)

require (
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674
	github.com/superradcompany/microsandbox/sdk/go v0.7.8 // indirect
)

replace github.com/floci-io/floci/tools/microsandbox-runtime => ../microsandbox-runtime
