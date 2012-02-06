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
 * Using the Riak-js library (https://github.com/frank06/riak-js).
 *
 * This seems to work in node 0.4.x, but not at all in 0.6.x (runs 5 operations
 * before encountering a socket hang up). Maybe it's the same reason the
 * Restler library fails - node 0.6.x defaults to using a "Connection:
 * Keep-Alive" header (see comments in riak_db.restler.js).
 */
var riak = require("riak-js");
var async = require("async");

exports.database = function(settings)
{
  // Some useful settings: host, port, api, clientId, bucket
  // See riak-js docs for all settings
  this.settings = settings;

  if (!settings.bucket)
    this.settings.bucket = "store";

  this.db = riak.getClient(this.settings);

  this.settings.cache = 1000;
  this.settings.writeInterval = 0;
  this.settings.json = false;
}

exports.database.prototype.init = function(callback)
{
  callback();
}

exports.database.prototype.get = function (key, callback)
{
  this.db.get(this.settings.bucket, key, {}, callback);
}

exports.database.prototype.set = function (key, value, callback)
{
  this.db.save(this.settings.bucket, key, value, {}, callback);
}

exports.database.prototype.remove = function (key, callback)
{
  this.db.remove(this.settings.bucket, key, {}, callback);
}

exports.database.prototype.doBulk = function (bulk, callback)
{ 
  var cb_err;

  for(var i in bulk)
  {  
    if(bulk[i].type == "set")
    {
      this.db.save(this.settings.bucket, bulk[i].key, bulk[i].value, {}, function(err) {
        cb_err = err;
      });
    }
    else if(bulk[i].type == "remove")
    {
      this.db.remove(this.settings.bucket, bulk[i].key, {}, function(err) {
        cb_err = err;
      });
    }
  }

  // will run the callback with the last error returned
  callback(cb_err);
}

exports.database.prototype.close = function(callback)
{
  callback();
}
