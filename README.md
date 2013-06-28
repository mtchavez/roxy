# Roxy

#### A Riak Proxy
A proxy between your client and Riak. Harnesses Go routines and a pool of connections to
Riak to handle your queries to Riak.

## Install

```
go get -u github.com/mtchavez/roxy
```

## Usage

### Configuration
Roxy uses a [toml](https://github.com/mojombo/toml) config file for settings. Here is an example config:

```
title = "Roxy configuration TOML"

[roxy]
  ip = "127.0.0.1"
  port = 8088
  p95 = 0.01
  bg_procs = 10

[riak]
  ip = "127.0.0.1"
  port = 8087
  pool_size = 5

[statsite]
  enabled = false
  ip = "0.0.0.0"
  port = 8125
```

#### [roxy]

* ```ip``` - IP you want Roxy to listen on
* ```port``` - Port you want for Roxy to listen on
* ```p95``` - Float number in milliseconds. Used to time out reads to Riak after
p95 time. Roxy will re-issue the read again and respond to the client.
* ```bg_procs``` - Threshold of total number of background PutReq that can run at one time. Otherwise runs normally.

#### [riak]

* ```ip``` - IP to your Riak
* ```port``` - Protocol Buffer Port for Riak
* ```pool_size``` - Number of connections for Roxy to make to Riak for a connection pool

#### [statsite]

* ```enabled``` - true to enable tracing stats via statsite or false to turn off
* ```ip``` - IP of statsite
* ```port``` - Port of statsite

### Starting a server

Starting Roxy just takes a path to a toml config

```go
package main

import "github.com/mtchavez/roxy"

func main() {
  roxy.Setup("./path/to/my-config.toml")
  roxy.RunProxy()
}
```

This will setup a ```net.Listener``` on the supplied ip and port from your config for
Roxy to listen for new connections on.

### Sending Requests
If you point your client to the Roxy ip and port you can now make regular Riak
requests as normal. A python example using the default Roxy ip and port would be:

```python
from riak import RiakClient, RiakPbcTransport, RiakObject
import simplejson


host = '127.0.0.1'
port = 8088
client = RiakClient(host=host, port=port, transport_class=RiakPbcTransport, transport_options={'timeout': 10, 'max_attempts': 3})

bucket = client.bucket('sample_bucket')
for i in xrange(1, 10):
  obj = RiakObject(client, bucket, 'user_%d' % i)
  obj._encode_data = False
  obj.set_content_type("application/json")
  doc = {'name': 'John Smith', 'age': 28 + i, 'company': 'Mr. Startup!'}
  obj.set_data(simplejson.dumps(doc))
  obj.store(w=1)
```

## Documentation

Docs can be found on [godoc](http://godoc.org/github.com/mtchavez/roxy/roxy) or
run ```go doc github.com/mtchavez/roxy``` locally

## Test

You can run the tests using the Makefile with ```make test``` or
using ```go test``` in the roxy directory. This currently expects Riak to be running
and available on default pb port ```8087```

## TODO

* p95 backup requests
  - Currently turned off
  - Need to figure out intermittent hanging when this is turned on

## License
Written by Chavez

Released under the MIT License: http://www.opensource.org/licenses/mit-license.php
