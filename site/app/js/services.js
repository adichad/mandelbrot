'use strict';

/* Services */

var mandelbrotServices = angular.module('mandelbrotServices', ['ngResource']);

mandelbrotServices.factory('HitService', 
  function($http) {
    this.search = function(kw, city, area) {
      return $http.get('http://138.91.35.108:9999/search/askme/place', {
          params: {
            kw: $kw, 	
            city: $city, 
            area: $area, 
            select: 'Area,LocationName,CompanyName,CompanyDescription,Product.cat3,Product.id,LatLong,City,CustomerType'
          }
        });
      }
  }
);

