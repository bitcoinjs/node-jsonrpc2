# node-jsonrpc2

This is a JSON-RPC server and client library for node.js <http://nodejs.org/>,
the V8 based evented IO framework.

## Install

To install node-jsonrpc2 in the current directory, run:

    npm install jsonrpc2

## Usage

Firing up an efficient JSON-RPC server becomes extremely simple:

``` javascript
var rpc = require('jsonrpc2');

var server = new rpc.Server();

function add(args, opt, callback) {
  callback(null, args[0] + args[1]);
}
server.expose('add', add);

server.listen(8000, 'localhost');
```

And creating a client to speak to that server is easy too:

``` javascript
var rpc = require('jsonrpc2');
var util = require('util');

var client = new rpc.Client(8000, 'localhost');

client.call('add', [1, 2], function(err, result) {
    util.puts('1 + 2 = ' + result);
});
```

To learn more, see the examples directory, peruse test/jsonrpc-test.js, or
simply "Use The Source, Luke".

More documentation and development is on its way.
