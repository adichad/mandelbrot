'use strict';

/* App Module */

var mandelbrotApp = angular.module('mandelbrotApp', [
  'ngRoute',
  'infinite-scroll',
  'mandelbrotAnimations',
  'highcharts-ng',
  'mandelbrotControllers',
  'mandelbrotFilters',
  'mandelbrotServices',
  'leaflet-directive'
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
