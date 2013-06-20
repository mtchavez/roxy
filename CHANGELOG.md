## v0.4.1 (2013-06-20)

* Add bg_procs as roxy option in config
* Temporarily disable p95 backup request feature
* Track total bg processes and throughput separately

## v0.4.0 (2013-06-19)

* Refactor of request handling
* Added ClientHandler to read and make requests

## v0.3.0 (2013-06-13)

* Add p95 threshold for reads
  * Will kill request and re-write/read
* Fix issues with reading long riak messages
  * Read in chunks of max response of 8192

## v0.2.1 (2013-06-07)

* Fix background buffer copying
* Track total throughput of bg processes with Statsite

## v0.2.0 (2013-06-05)

* Fix version string parsing to default to false
* Fix memory leak with go routines from RequestHandler not dying
* Added backgroundwrite.go to handle
  * Counting of background writes
  * Writes up to a threshold in the background
  * Quick put responses to client and writes in background now
  * Put response is not the doc but is { "roxy": true }

## v0.1.0 (2013-06-04)

* Toml config parsing
* RiakPool
* Request read/write with client
* Request read/write with Riak
* RiakServer listener
* Signal trapping to shutdown gracefully
* Optional statsite integration
