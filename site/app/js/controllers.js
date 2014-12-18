'use strict';

/* Controllers */

var mandelbrotControllers = angular.module('mandelbrotControllers', []);

mandelbrotControllers.controller('HitListCtrl',
  function($scope, $http) {
    $scope.search = function() {
      // $scope.hits = [{fields: {placename: ['place1'], compdesc: ['compdesc1']}}];
      $scope.hello={results: ['something', 's2']};

      $http.jsonp('http://192.168.1.139:9999/search/askme/place?callback=JSON_CALLBACK', {
          'params': {
            'kw': $scope.kw,
            'city': $scope.city,
            'area': $scope.area,
            'select': 'Area,LocationName,CompanyName,CompanyDescription,Product.cat3,Product.id,LatLong,City,CustomerType'
          }
        }).success(function(data,status,headers,config) {
          $scope.hits = data.results.hits.hits;
          console.log(data);
        }).error(function(data,status,headers,config) {
          $scope.hits = status;
          console.log(status);
        });

    }
  }
);

