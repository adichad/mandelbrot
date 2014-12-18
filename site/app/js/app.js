'use strict';

/* App Module */

var mandelbrotApp = angular.module('mandelbrotApp', [
  'ngRoute',
  'mandelbrotAnimations',

  'mandelbrotControllers',
  'mandelbrotFilters',
  'mandelbrotServices'
]);

mandelbrotApp.config(['$routeProvider',
  function($routeProvider) {
    $routeProvider.
      when('/hits', {
        templateUrl: 'partials/hit-list.html',
        controller: 'HitListCtrl'
      }).
      otherwise({
        redirectTo: '/hits'
      });
  } 
  ]);
