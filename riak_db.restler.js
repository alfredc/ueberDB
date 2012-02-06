/**
 * 2012 Alfred Chan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Using the Restler library (https://github.com/danwrong/restler).
 *
 * For some reason, the benchmark doesn't work when delete operations are
 * included, UNLESS (1) the operations are not randomized, AND (2) there are
 * less than 5 deletes. Why??
 *
 * Also, when running a batch of operations in the node console, only about
 * half the operations seem to go through UNLESS request.shouldKeepAlive is
 * set to false (see commented out blocks). Why would this happen only in the
 * console?
 *
 * I think this works better in node 0.4.x.
 */
var rest = require("restler");
var async = require("async");

exports.database = function(settings)
{
  // Settings: host, port, bucket
  this.settings = settings;

  if (this.settings.host)
  {
    this.settings.url = this.settings.host;

    if (this.settings.url.slice(0,7) != "http://")
      this.settings.url = "http://" + this.settings.url;

    if (this.settings.port)
      this.settings.url += ":" + this.settings.port;
  }

  if (!settings.bucket)
    this.settings.bucket = "store";

  this.settings.cache = 1000;
  this.settings.writeInterval = 0;
  this.settings.json = false;
}

exports.database.prototype.init = function(callback)
{
  callback(null);
}

exports.database.prototype.get = function (key, callback)
{
  /*
  var req = new rest.Request(
    this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key,
    { method: 'GET' });
  req.on('complete', function(data, res) {
    if (data instanceof Error)
      callback(data, res)
    else
      callback(null, data)
  });
  req.request.shouldKeepAlive = false;
  req.run();
  */
  rest.get(
    this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key
  ).on('complete', getCallback(callback));
}

exports.database.prototype.set = function (key, value, callback)
{
  var contentType;
  if (typeof value === 'object')
  {
    value = JSON.stringify(value);
    contentType = 'application/json';
  }
  else if (value instanceof Buffer)
  {
    contentType = 'application/octet-stream';
  }
  else
  {
    contentType = 'text/plain';
  }

  /*
  var req = new rest.Request(
    this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key,
    { method: 'PUT',
      data: value,
      headers: { 'Content-Type': contentType }
    });
  req.on('complete', function(data, res) {
    if (data instanceof Error)
      callback(data, res)
    else
      callback(null, data)
  });
  req.request.shouldKeepAlive = false;
  req.run();
  */
  rest.put(
    this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key,
    { data: value, headers: { 'Content-Type': contentType } }
  ).on('complete', getCallback(callback));
}

exports.database.prototype.remove = function (key, callback)
{
  /*
  var req = new rest.Request(
    this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key,
    { method: 'DELETE' });
  req.on('complete', function(data, res) {
    if (data instanceof Error)
      callback(data, res)
    else
      callback(null, data)
  });
  req.request.shouldKeepAlive = false;
  req.run();
  */
  rest.del(
    this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key
  ).on('complete', getCallback(callback));
}

exports.database.prototype.doBulk = function (bulk, callback)
{ 
  for(var i in bulk)
  {  
    if(bulk[i].type == "set")
    {
      this.set(bulk[i].key, bulk[i].value, callback);
    }
    else if(bulk[i].type == "remove")
    {
      this.remove(bulk[i].key, callback);
    }
  }
}

exports.database.prototype.close = function(callback)
{
  callback(null);
}

getCallback = function(callback)
{
  return function(data, res)
  {
    if (data instanceof Error)
      callback(data, res);
    else if (400 <= res.statusCode && res.statusCode <= 599)
    {
      console.log(res);
      if (res.statusCode == 404)
        callback(null, null);
      else
      {
        var err = new Error("HTTP error #{meta.statusCode}: #{data}");
        err.statusCode = res.statusCode;
        callback(err, data);
      }
    }
    else {
      callback(null, data);
    }
  }
}
