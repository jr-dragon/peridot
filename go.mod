module github.com/jr-dragon/peridot

go 1.27.3

require (
	github.com/jeremiah-masters/dlht v0.0.0-20260814071640-5c0560ab3ecb
	github.com/mazrean/kessoku v1.2.0
	github.com/puzpuzpuz/xsync/v4 v4.5.0
	github.com/zeebo/xxh3 v1.1.0
	golang.org/x/sync v0.24.0
)

require (
	github.com/alecthomas/kong v1.16.1 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/matryer/moq v0.8.1 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
)

tool (
	github.com/matryer/moq
	github.com/mazrean/kessoku/cmd/kessoku
)
