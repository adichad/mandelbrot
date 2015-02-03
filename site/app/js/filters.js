'use strict';

/* Filters */

angular.module('mandelbrotFilters', []).filter('checkmark', function() {
  return function(input) {
    return input ? '\u2713' : '\u2718';
  };
});
