'use strict';
var _ = require('lodash');
var trackers = {};
var measuring = 0;

function count(key) {
  if (!key) {
    return measuring;
  } else {
    return _.keys(trackers[key]).length;
  }
}

function time(key) {
  var hash = Math.random().toString(16).split('.').pop();
  if (! trackers[key]) {
    trackers[key] = {};
  }
  trackers[key][hash] = {
    startAt: Date.now()
  };

  measuring++;
  return {
    key: key,
    hash: hash,
    count: count,
    end: _.partial(timeEnd, key, hash)
  };
}

function timeEnd(key, hash) {
  measuring--;
  if (trackers[key] && trackers[key][hash]) {
    var result = Date.now() - trackers[key][hash].startAt;
    delete trackers[key][hash];
    return result;
  }
}

module.exports = { time: time };