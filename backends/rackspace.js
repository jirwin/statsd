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
var _ = require('underscore');

function RackspaceBackend(startupTime, config, stats){
  var self = this;

  this.lastFlush = startupTime;
  this.lastException = startupTime;
  this.config = config.rax || {};
  this.filename = path.join(this.config.outputDir, startupTime + ".json");

  this.statsCache = {
    counters: {},
    timers: {}
  };

  stats.on('flush', self.flush.bind(self));
  stats.on('status', self.status.bind(self));
}

/**
  * Clear the metrics cache
  * @param {Metrics object} metrics Metrics provided by statsd.
  */
RackspaceBackend.prototype.clearMetrics = function(metrics) {
  var self = this;

  _.each(self.statsCache, function(val, type) {
    if(!metrics[type]) {
      return;
    }

    _.each(metrics[type], function(val, name) {
      self.statsCache[type][name] = 0;
    });
  });
};


RackspaceBackend.prototype.flush = function(timestamp, metrics) {
  var self = this,
      out;

  console.log('Flushing stats at', new Date(timestamp * 1000).toString());

  _.each(self.statsCache, function(type) {
    if(!metrics[type]) return;

    _.each(metrics[type], function(name) {
      var value = metrics[type][name];
      if(!self.statsCache[type][name]){
        self.statsCache[type][name] = 0;
      }
      self.statsCache[type][name] = (self.statsCache[type][name] + value) / 2;
    });
  });

  if ((timestamp - this.lastFlush) < this.config.flushInterval) {
    return;
  }

  out = {
    counters: this.statsCache.counters,
    timers: this.statsCache.timers,
    gauges: metrics.gauges,
    timer_data: metrics.timer_data,
    counter_rates: metrics.counter_rates,
    sets: function (vals) {
      var ret = {};
      for (var val in vals) {
        ret[val] = vals[val].values();
      }
      return ret;
    }(metrics.sets),
    pctThreshold: metrics.pctThreshold
  };

  fs.appendFileSync(self.filename, JSON.stringify(out));
  self.lastFlush = timestamp;
  self.clearMetrics(metrics);

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
