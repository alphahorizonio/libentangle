# libentangle

A package to build peer-to-peer file sharing solutions.

[![Go Reference](https://pkg.go.dev/badge/github.com/alphahorizonio/libentangle.svg)](https://pkg.go.dev/github.com/alphahorizonio/libentangle)

## Overview

`libentangle` is a package containing a webrtc signaling server, clients and an implementation build upon that to handle files via P2P connection from remote using an extended `io.ReadWriteSeeker`. This interface can be used by any implementation which supports this basic API and will therefore allow working with a remote file. 

## Installation 

First use `go get` to install the latest version of the package.

```bash
$ go get github.com/alphahorizonio/libentangle
```

Next, include `libentangle` in your application.

```go
import "github.com/alphahorizonio/libentangle"
```

## Usage 

For a detailed example, please look at the [`client.go`](https://github.com/alphahorizonio/libentangle/blob/main/cmd/libentangle/cmd/client.go) command. 

## Contributing 

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am "feat: Add something"`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create Pull Request

## License 

libentangle (c) 2022 Jakob Waibel and contributors

SPDX-License-Identifier: AGPL-3.0
