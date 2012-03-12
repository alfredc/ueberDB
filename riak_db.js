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
 * Using the Request library (https://github.com/mikeal/request).
 *
 * For some reason, it works much slower (about 7x longer per operation) in
 * node 0.6.x than 0.4.x.
 */
var request = require("request");
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

  this.settings.cache = 0;
  this.settings.writeInterval = 0;
  this.settings.json = false;
}

exports.database.prototype.init = function(callback)
{
  callback();
}

exports.database.prototype.get = function (key, callback)
{
  request({ method: 'GET',
            uri: this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key
          }, getCallback(callback));
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

  request({ method: 'PUT',
            uri: this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key,
            body: value,
            headers: { 'Content-Type': contentType }
          }, getCallback(callback));
}

exports.database.prototype.remove = function (key, callback)
{
  request({ method: 'DELETE',
            uri: this.settings.url + '/buckets/' + this.settings.bucket + '/keys/' + key
          }, getCallback(callback));
}

exports.database.prototype.doBulk = function (bulk, callback)
{ 
  var cb_err;

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

  // will run the callback with the last error returned
  callback(cb_err);
}

exports.database.prototype.close = function(callback)
{
  callback();
}

getCallback = function(callback)
{
  return function(err, res, body)
  {
    if (400 <= res.statusCode && res.statusCode <= 599)
    {
      if (res.statusCode == 404)
        callback(null, null);
      else
      {
        var http_err = new Error("HTTP error #{meta.statusCode}: #{data}");
        http_err.statusCode = res.statusCode;
        callback(http_err, body);
      }
    }
    else
    {
      try {
        body = JSON.parse(body);
      } catch (e) {}
      callback(err, body);
    }
  }
}
