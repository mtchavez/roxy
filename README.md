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

[statsite]
  enabled = false
  ip = "0.0.0.0"
  port = 8125

[roxy]
  ip = "127.0.0.1"
  port = 8088

[riak]
  ip = "127.0.0.1"
  port = 8087
  pool_size = 5
```
Statsite can be enabled with ```enabled = true``` if you want to capture Roxy
stats.

Pool size is used to configure how many connections to Riak you want Roxy to
create and use.

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

Docs can be found on [godoc](http://godoc.org/github.com/mtchavez/roxy) or
run ```go doc github.com/mtchavez/roxy``` locally

## Test

You can run the tests using the Makefile with ```make test``` or
using ```go test``` in the roxy directory. This currently expects Riak to be running
and available on default pb port ```8087```

## TODO

* Enable setting threshold of writing in background
  - Setting this to 0 would turn off quick put responses and background writing
  - This would be needed for people who expect the saved doc back on a put request
* Given a %95 for request times
  - Time out Riak requests if over %95 time
  - Re-try same request to Riak

## License
Written by Chavez

Released under the MIT License: http://www.opensource.org/licenses/mit-license.php
