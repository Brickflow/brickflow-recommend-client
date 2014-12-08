'use strict';
var _ = require('lodash');
var factory = require('amqp-rpc').factory;
var defaultLogger = require('./utils/dummyLogger');
var measure = require('./utils/measure');

var ACTIONS = [
  'register',
  'updateFeedCache',
  'updateYourCache',
  'updateBlogCache',
  'updateBlogFallbackCache',
  'updateTrendingCache'
];

var instances = {};
function getRPC(options) {
  if (!instances[options.url]) {
    instances[options.url] = factory(options);
  }
  return instances[options.url];
}

module.exports = function createClient(options) {
  options = _.defaults(options || {}, {
    exchange: 'recommend_rpc_exchange',
    queueName: 'recommend-rpc',
    url: 'amqp://guest:guest@localhost:5672'
  });
  var logger = options.logger || defaultLogger;

  var rpc = getRPC(_.pick(options, 'url', 'exchange'));

  function query(action) {
    var args = Array.prototype.slice.call(arguments);
    var hasCb = typeof _.last(args) === 'function';
    var cb = hasCb ? _.last(args) : _.noop;
    var params = args.slice(1, hasCb ? -1 : undefined);
    var rpcParams = { action: action, params: params };
    logger.info(options.queueName + '-call', rpcParams);
    var dt = measure.time('recommend-rpc-client');
    rpc.call(options.queueName, rpcParams, function(err, res) {
      logger.info(options.queueName + '-response', _.assign(rpcParams, {
        running: dt.count('recommend-rpc-client'),
        duration: dt.end()
      }));
      cb(err, res);
    });
  }

  return _(ACTIONS).zipObject().mapValues(function (x, action) {
    return _.partial(query, action);
  }).value();
};


