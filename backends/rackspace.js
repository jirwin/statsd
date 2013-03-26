/*
 * Flush stats to disk for the rackspace monitoring agent.
 *
 * To enable this backend, include 'rackspace' in the backends
 * configuration array:
 *
 *   backends: ['rackspace']
 *
 * This backend supports the following config options:
 *
 *   flushInterval: The number of seconds between metric flushes
 *   outputDir: The directory to place metrics on disk
 */

var fs = require('fs');
var path = require('path');



function RackspaceBackend(startupTime, config, stats){
  var self = this;
  this.lastFlush = startupTime;
  this.lastException = startupTime;
  this.config = config.rax || {};
  this.flush

  this.statsCache = {
    counters: {},
    timers: {}
  };

  stats.on('flush', function(timestamp, metrics) { self.flush(timestamp, metrics); });
  stats.on('status', function(callback) { self.status(callback); });
};

RackspaceBackend.prototype.flush = function(timestamp, metrics) {
  var self = this;

  console.log('Flushing stats at', new Date(timestamp * 1000).toString());

  Object.keys(self.statsCache).forEach(function(type) {
    if(!metrics[type]) return;
    Object.keys(metrics[type]).forEach(function(name) {
      var value = metrics[type][name];
      self.statsCache[type][name] || (self.statsCache[type][name] = 0);
      self.statsCache[type][name] += value;
    });
  });

  var out = {
    counters: this.statsCache.counters,
    timers: this.statsCache.timers,
    gauges: metrics.gauges,
    timer_data: metrics.timer_data,
    counter_rates: metrics.counter_rates,
    sets: function (vals) {
      var ret = {};
      for (val in vals) {
        ret[val] = vals[val].values();
      }
      return ret;
    }(metrics.sets),
    pctThreshold: metrics.pctThreshold
  };

  fs.appendFile(path.join(this.config.rackspaceOutDir, timestamp + ".json"), JSON.stringify(out), function(err) {
    if (err) throw err;

    console.log("flushed stats to file.");
  });
};

RackspaceBackend.prototype.status = function(write) {
  ['lastFlush', 'lastException'].forEach(function(key) {
    write(null, 'console', key, this[key]);
  }, this);
};

exports.init = function(startupTime, config, events) {
  var instance = new RackspaceBackend(startupTime, config, events);
  return true;
};
