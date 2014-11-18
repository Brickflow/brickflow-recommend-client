var _ = require('lodash');

module.exports = _([
  'info',
  'error',
  'debug',
  'warn'
]).zipObject().mapValues(function(x, level) {
  return function() {
    console.log.apply(console, [level + ':'].concat(_.values(arguments)));
  };
}).value();