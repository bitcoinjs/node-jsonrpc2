var util = require('util');
var net = require('net');
var http = require('http');
var util = require('util');
var events = require('events');
var JsonParser = require('jsonparse');

var UNAUTHORIZED = "Unauthorized\n";
var METHOD_NOT_ALLOWED = "Method Not Allowed\n";
var INVALID_REQUEST = "Invalid Request\n";

/**
 * Abstract base class for RPC endpoints.
 *
 * Has the ability to register RPC events and expose RPC methods.
 */
var Endpoint = function ()
{
  events.EventEmitter.call(this);

  this.functions = {};
  this.scopes = {};
  this.defaultScope = this;
};
util.inherits(Endpoint, events.EventEmitter);

/**
 * Output a piece of debug information.
 */
Endpoint.trace = function(direction, message)
{
  console.log('   ' + direction + '   ' + message);
}

/**
 * Define a callable method on this RPC endpoint
 */
Endpoint.prototype.expose = function(name, func, scope)
{
  if ("function" === typeof func) {
    Endpoint.trace('***', 'exposing: ' + name);
    this.functions[name] = func;

    if (scope) {
      this.scopes[name] = scope;
    }
  } else {
    var funcs = [];
    var object = func;
    for(var funcName in object) {
      var funcObj = object[funcName];
      if(typeof(funcObj) == 'function') {
        this.functions[name + '.' + funcName] = funcObj;
        funcs.push(funcName);

        if (scope) {
          this.scopes[name + '.' + funcName] = scope;
        }
      }
    }
    Endpoint.trace('***', 'exposing module: ' + name +
                   ' [funs: ' + funcs.join(', ') + ']');
    return object;
  }
}

/**
 * Handle a call to one of the endpoint's methods.
 */
Endpoint.prototype.handleCall = function handleCall(decoded, conn, callback)
{
  Endpoint.trace('<--', 'Request (id ' + decoded.id + '): ' + 
                 decoded.method + '(' + decoded.params.join(', ') + ')');

  if (!this.functions.hasOwnProperty(decoded.method)) {
    callback(new Error("Unknown RPC call '"+decoded.method+"'"));
    return;
  }

  var method = this.functions[decoded.method];
  var scope = this.scopes[decoded.method] || this.defaultScope;

  // Try to call the method, but intercept errors and call our
  // error handler.
  try {
    method.call(scope, decoded.params, conn, callback);
  } catch (err) {
    callback(err);
  }
};

Endpoint.prototype.exposeModule = Endpoint.prototype.expose;

/**
 * JSON-RPC Client.
 */
var Client = function (port, host, user, password)
{
  Endpoint.call(this);

  this.port = port;
  this.host = host;
  this.user = user;
  this.password = password;
};

util.inherits(Client, Endpoint);


/**
 * Make HTTP connection/request.
 *
 * In HTTP mode, we get to submit exactly one message and receive up to n
 * messages.
 */
Client.prototype.connectHttp = function connectHttp(method, params, opts, callback)
{
  if ("function" === typeof opts) {
    callback = opts;
    opts = {};
  }
  opts = opts || {};

  var client = http.createClient(this.port, this.host);

  var id = 1;

  // First we encode the request into JSON
  var requestJSON = JSON.stringify({
    'id': id,
    'method': method,
    'params': params,
    'jsonrpc': '2.0'
  });

  // Report errors from the http client. This also prevents crashes since
  // an exception is thrown if we don't handle this event.
  client.on('error', function(err) {
    callback(err);
  });

  var headers = {};

  if (this.user && this.password) {
    var buff = new Buffer(this.user + ":" + this.password).toString('base64');
    var auth = 'Basic ' + buff;
    headers['Authorization'] = auth;
  }

  // Then we build some basic headers.
  headers['Host'] = this.host;
  headers['Content-Length'] = Buffer.byteLength(requestJSON, 'utf8');

  // Now we'll make a request to the server
  var request = client.request('POST', opts.path || '/', headers);
  request.write(requestJSON);
  request.on('response', callback.bind(this, id, request));
};

/**
 * Make Socket connection.
 *
 * This implements JSON-RPC over a raw socket. This mode allows us to send and
 * receive as many messages as we like once the socket is established.
 */
Client.prototype.connectSocket = function connectSocket(callback)
{
  var self = this;

  var socket = net.connect(this.port, this.host, function () {
    // Submit non-standard "auth" message for raw sockets.
    if ("string" === typeof self.user &&
        "string" === typeof self.password) {
      conn.call("auth", [self.user, self.password], function (err) {
        if (err) {
          callback(err);
        } else {
          callback(null, conn);
        }
      });
      return;
    }
    if ("function" === typeof callback) {
      callback(null, conn);
    }
  });
  var conn = new SocketConnection(self, socket);
  var parser = new JsonParser();
  parser.onValue = function (decoded) {
    if (this.stack.length) return;

    conn.handleMessage(decoded);
  };
  socket.on('data', function (chunk) {
    try {
      parser.write(chunk);
    } catch(err) {
      Endpoint.trace('<--', err.toString());
    }
  });

  return conn;
};

Client.prototype.stream = function (method, params, opts, callback)
{
  if ("function" === typeof opts) {
    callback = opts;
    opts = {};
  }
  opts = opts || {};

  this.connectHttp(method, params, opts, function (id, request, response) {
    if ("function" === typeof callback) {
      var connection = new events.EventEmitter();
      connection.id = id;
      connection.req = request;
      connection.res = response;
      connection.expose = function (method, callback) {
        connection.on('call:'+method, function (data) {
          callback.call(null, data.params || []);
        });
      };
      connection.end = function () {
        this.req.connection.end();
      };

      // We need to buffer the response chunks in a nonblocking way.
      var parser = new JsonParser();
      parser.onValue = function (decoded) {
        if (this.stack.length) return;

        connection.emit('data', decoded);
        if (decoded.hasOwnProperty('result') || 
            decoded.hasOwnProperty('error') &&
            decoded.id === id &&
            "function" === typeof callback) {
          connection.emit('result', decoded);
        } else if (decoded.hasOwnProperty('method')) {
          connection.emit('call:'+decoded.method, decoded);
        }
      };
      // Handle headers
      connection.res.once('data', function (data) {
        if (connection.res.statusCode === 200) {
          callback(null, connection);
        } else {
          callback(new Error(""+connection.res.statusCode+" "+data));
        }
      });
      connection.res.on('data', function (chunk) {
        try {
          parser.write(chunk);
        } catch(err) {
          // TODO: Is ignoring invalid data the right thing to do?
        }
      });
      connection.res.on('end', function () {
        // TODO: Issue an error if there has been no valid response message
      });
    }
  });
};

Client.prototype.call = function (method, params, opts, callback)
{
  if ("function" === typeof opts) {
    callback = opts;
    opts = {};
  }
  opts = opts || {};
  this.connectHttp(method, params, opts, function (id, request, response) {
    var data = '';
    response.on('data', function (chunk) {
      data += chunk;
    });
    response.on('end', function () {
      if (response.statusCode !== 200) {
        callback(new Error(""+response.statusCode+" "+data));
        return;
      }
      var decoded = JSON.parse(data);
      if ("function" === typeof callback) {
        if (!decoded.error) {
          decoded.error = null;
        }
        callback(decoded.error, decoded.result);
      }
    });
  });
};

/**
 * JSON-RPC Server.
 */
function Server(opts) {
  Endpoint.call(this);

  opts = opts || {};
  opts.type = opts.type || 'http';
}
util.inherits(Server, Endpoint);

/**
 * Start listening to incoming connections.
 */
Server.prototype.listen = function listen(port, host)
{
  var server = http.createServer(this.handleHttp.bind(this));
  server.listen(port, host);
  Endpoint.trace('***', 'Server listening on http://' +
                 (host || '127.0.0.1') + ':' + port + '/');
  return server;
}

Server.prototype.listenRaw = function listenRaw(port, host)
{
  var server = net.createServer(this.handleRaw.bind(this));
  server.listen(port, host);
  Endpoint.trace('***', 'Server listening on socket://' +
                 (host || '127.0.0.1') + ':' + port + '/');
  return server;
};

Server.prototype.listenHybrid = function listenHybrid(port, host) {
  var httpServer = http.createServer(this.handleHttp.bind(this));
  var server = net.createServer(this.handleHybrid.bind(this, httpServer));
  server.listen(port, host);
  Endpoint.trace('***', 'Server (hybrid) listening on socket://' +
                 (host || '127.0.0.1') + ':' + port + '/');
  return server;
};

/**
 * Handle a low level server error.
 */
Server.handleHttpError = function(req, res, code, message)
{
  var headers = {'Content-Type': 'text/plain',
                 'Content-Length': message.length,
                 'Allow': 'POST'};

  if (code === 401) {
    headers['WWW-Authenticate'] = 'Basic realm="JSON-RPC"';
  }

  res.writeHead(code, headers);
  res.write(message);
  res.end();
};

/**
 * Handle HTTP POST request.
 */
Server.prototype.handleHttp = function(req, res)
{
  Endpoint.trace('<--', 'Accepted http request');

  if (req.method !== 'POST') {
    Server.handleHttpError(req, res, 405, METHOD_NOT_ALLOWED);
    return;
  }

  var buffer = '';
  var self = this;

  // Check authentication if we require it
  if (this.authHandler) {
    var authHeader = req.headers['authorization'] || '',   // get the header
        authToken = authHeader.split(/\s+/).pop() || '',   // get the token
        auth = new Buffer(authToken, 'base64').toString(), // base64 -> string
        parts = auth.split(/:/),                           // split on colon
        username = parts[0],
        password = parts[1];
    if (!this.authHandler(username, password)) {
      Server.handleHttpError(req, res, 401, UNAUTHORIZED);
      return;
    }
  }

  var handle = function (buf) {
    var decoded = JSON.parse(buf);

    // Check for the required fields, and if they aren't there, then
    // dispatch to the handleHttpError function.
    if (!(decoded.method && decoded.params && decoded.id)) {
      Endpoint.trace('-->', 'Response (invalid request)');
      Server.handleHttpError(req, res, 400, INVALID_REQUEST);
      return;
    }

    var reply = function (json) {
      var encoded = JSON.stringify(json);

      if (!conn.isStreaming) {
        res.writeHead(200, {'Content-Type': 'application/json',
                            'Content-Length': encoded.length});
        res.write(encoded);
        res.end();
      } else {
        res.writeHead(200, {'Content-Type': 'application/json'});
        res.write(encoded);
        // Keep connection open
      }
    };

    var callback = function(err, result) {
      if (err) {
        Endpoint.trace('-->', 'Failure (id ' + decoded.id + '): ' +
                       (err.stack ? err.stack : err.toString()));
        err = err.toString();
        result = null;
      } else {
        Endpoint.trace('-->', 'Response (id ' + decoded.id + '): ' +
                       JSON.stringify(result));
        err = null;
      }

      // TODO: Not sure if we should return a message if decoded.id == null
      reply({
        'result': result,
        'error': err,
        'id': decoded.id
      });
    };

    var conn = new HttpServerConnection(self, req, res);

    self.handleCall(decoded, conn, callback);
  }; // function handle(buf)

  req.addListener('data', function(chunk) {
    buffer = buffer + chunk;
  });

  req.addListener('end', function() {
    handle(buffer);
  });
};

Server.prototype.handleRaw = function handleRaw(socket)
{
  Endpoint.trace('<--', 'Accepted socket connection');

  var self = this;

  var conn = new SocketConnection(this, socket);
  var parser = new JsonParser();
  var requireAuth = !!this.authHandler;

  parser.onValue = function (decoded) {
    if (this.stack.length) return;

    // We're on a raw TCP socket. To enable authentication we implement a simple
    // authentication scheme that is non-standard, but is easy to call from any
    // client library.
    //
    // The authentication message is to be sent as follows:
    //   {"method": "auth", "params": ["myuser", "mypass"], id: 0}
    if (requireAuth) {
      if (decoded.method !== "auth" ) {
        // Try to notify client about failure to authenticate
        if ("number" === typeof decoded.id) {
          conn.sendReply("Error: Unauthorized", null, decoded.id);
        }
      } else {
        // Handle "auth" message
        if (Array.isArray(decoded.params) &&
            decoded.params.length === 2 &&
            self.authHandler(decoded.params[0], decoded.params[1])) {
          // Authorization completed
          requireAuth = false;

          // Notify client about success
          if ("number" === typeof decoded.id) {
            conn.sendReply(null, true, decoded.id);
          }
        } else {
          if ("number" === typeof decoded.id) {
            conn.sendReply("Error: Invalid credentials", null, decoded.id);
          }
        }
      }
      // Make sure we explicitly return here - the client was not yet auth'd.
      return;
    } else {
      conn.handleMessage(decoded);
    }
  };

  socket.on('data', function (chunk) {
    try {
      parser.write(chunk);
    } catch(err) {
      // TODO: Is ignoring invalid data the right thing to do?
    }
  });
};

Server.prototype.handleHybrid = function handleHybrid(httpServer, socket)
{
  var self = this;
  socket.once('data', function (chunk) {
    // If first byte is a capital letter, treat connection as HTTP
    if (chunk[0] >= 65 && chunk[0] <= 90) {
      httpServer.emit('connection', socket);
    } else {
      self.handleRaw(socket);
    }
    // Re-emit first chunk
    socket.emit('data', chunk);
  });
};

/**
 * Set the server to require authentication.
 *
 * Can be called with a custom handler function:
 *   server.enableAuth(function (user, password) {
 *     return true; // Do authentication and return result as boolean
 *   });
 * 
 * Or just with a single valid username and password:
 *   sever.enableAuth("myuser", "supersecretpassword");
 */
Server.prototype.enableAuth = function enableAuth(handler, password) {
  if ("function" !== typeof handler) {
    var user = "" + handler;
    password = "" + password;
    handler = function checkAuth(suppliedUser, suppliedPassword) {
      return user === suppliedUser && password === suppliedPassword;
    };
  }

  this.authHandler = handler;
};

var Connection = function Connection(ep) {
  events.EventEmitter.call(this);

  this.endpoint = ep;
  this.callbacks = [];
  this.latestId = 0;

  // Default error handler (prevents "uncaught error event")
  this.on('error', function () {});
};

util.inherits(Connection, events.EventEmitter);

/**
 * Make a standard RPC call to the other endpoint.
 *
 * Note that some ways to make RPC calls bypass this method, for example HTTP
 * calls and responses are done in other places.
 */
Connection.prototype.call = function call(method, params, callback)
{
  if (!Array.isArray(params)) {
    params = [params];
  }

  var id = null;
  if ("function" === typeof callback) {
    id = ++this.latestId;
    this.callbacks[id] = callback;
  }

  Endpoint.trace('-->', 'Call (method '+method+'): ' + JSON.stringify(params));
  var data = JSON.stringify({
    method: method,
    params: params,
    id: id
  });
  this.write(data);
};

/**
 * Dummy method for sending data.
 *
 * Connection types that support sending additional data will override this
 * method.
 */
Connection.prototype.write = function write(data)
{
  throw new Error("Tried to write data on unsupported connection type.");
};

/**
 * Keep the connection open.
 *
 * This method is used to tell a HttpServerConnection to stay open. In order
 * to keep it compatible with other connection types, we add it here and make
 * it register a connection end handler.
 */
Connection.prototype.stream = function (onend)
{
  if ("function" === typeof onend) {
    this.on('end', onend);
  }
};

Connection.prototype.handleMessage = function handleMessage(msg)
{
  if (msg.hasOwnProperty('result') || 
      msg.hasOwnProperty('error') &&
      msg.hasOwnProperty('id') &&
      "function" === typeof this.callbacks[msg.id]) {
    try {
      this.callbacks[msg.id](msg.error, msg.result);
    } catch(err) {
      // TODO: What do we do with erroneous callbacks?
    }
  } else if (msg.hasOwnProperty('method')) {
    this.endpoint.handleCall(msg, this, (function (err, result) {
      if (err) {
        Endpoint.trace('-->', 'Failure (id ' + msg.id + '): ' +
                       (err.stack ? err.stack : err.toString()));
      }

      if ("undefined" === msg.id || null === msg.id) return;

      if (err) {
        err = err.toString();
        result = null;
      } else {
        Endpoint.trace('-->', 'Response (id ' + msg.id + '): ' +
                       JSON.stringify(result));
        err = null;
      }

      this.sendReply(err, result, msg.id);
    }).bind(this));
  }
};

Connection.prototype.sendReply = function sendReply(err, result, id) {
  var data = JSON.stringify({
    result: result,
    error: err,
    id: id
  });
  this.write(data);
};

var HttpServerConnection = function HttpServerConnection(server, req, res)
{
  Connection.call(this, server);

  var self = this;

  this.req = req;
  this.res = res;
  this.isStreaming = false;

  this.res.connection.on('end', function () {
    self.emit('end');
  });
};

util.inherits(HttpServerConnection, Connection);

/**
 * Can be called before the response callback to keep the connection open.
 */
HttpServerConnection.prototype.stream = function (onend)
{
  Connection.prototype.stream.call(this, onend);

  this.isStreaming = true;
};

/**
 * Send the client additional data.
 *
 * An HTTP connection can be kept open and additional RPC calls sent through if
 * the client supports it.
 */
HttpServerConnection.prototype.write = function (data)
{
  if (!this.isStreaming) {
    throw new Error("Cannot send extra messages via non-streaming HTTP");
  }

  if (!this.res.connection.writable) {
    // Client disconnected, we'll quietly fail
    return;
  }

  this.res.write(data);
};

/**
 * Socket connection.
 *
 * Socket connections are mostly symmetric, so we are using a single class for
 * representing both the server and client perspective.
 */
var SocketConnection = function SocketConnection(endpoint, conn)
{
  Connection.call(this, endpoint);

  var self = this;

  this.conn = conn;
  this.autoReconnect = true;
  this.ended = true;

  this.conn.on('connect', function () {
    self.emit('connect');
  });

  this.conn.on('end', function () {
    self.emit('end');
  });

  this.conn.on('error', function () {
    self.emit('error');
  });

  this.conn.on('close', function (hadError) {
    self.emit('close', hadError);

    // Handle automatic reconnections if we are the client
    if (self.endpoint instanceof Client &&
        self.autoReconnect &&
        !self.ended) {
      if (hadError) {
        // If there was an error, we'll wait a moment before retrying
        setTimeout(self.reconnect.bind(self), 200);
      } else {
        self.reconnect();
      }
    }
  });
};

util.inherits(SocketConnection, Connection);

SocketConnection.prototype.write = function write(data)
{
  if (!this.conn.writable) {
    // Other side disconnected, we'll quietly fail
    return;
  }

  this.conn.write(data);
};

SocketConnection.prototype.end = function end()
{
  this.ended = true;
  this.conn.end();
};

SocketConnection.prototype.reconnect = function reconnect()
{
  this.ended = false;
  if (this.endpoint instanceof Client) {
    this.conn.connect(this.endpoint.port, this.endpoint.host);
  } else {
    throw new Error('Cannot reconnect a connection from the server-side.');
  }
};

exports.Endpoint = Endpoint;
exports.Server = Server;
exports.Client = Client;

exports.Connection = Connection;
exports.HttpServerConnection = HttpServerConnection;
exports.SocketConnection = SocketConnection;
