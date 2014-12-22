'use strict';

/* Controllers */

var mandelbrotControllers = angular.module('mandelbrotControllers', []);

mandelbrotControllers.controller('HitListCtrl', ['$scope', 'leafletData', 'Mandelbrot',
  function($scope, leafletData, Mandelbrot) {
    $scope.mandelbrot = new Mandelbrot();
    leafletData.getMap('map').then(function(map) {
        var options = { fill: false, weight: 2 }
        L.circle([$scope.mandelbrot.center.lat, $scope.mandelbrot.center.lng], 1500, options).addTo(map);
        L.circle([$scope.mandelbrot.center.lat, $scope.mandelbrot.center.lng], 4000, options).addTo(map);
        L.circle([$scope.mandelbrot.center.lat, $scope.mandelbrot.center.lng], 8000, options).addTo(map);
        L.circle([$scope.mandelbrot.center.lat, $scope.mandelbrot.center.lng], 30000, options).addTo(map);
      });
  }]);

mandelbrotApp.factory('PieChart', function() {
    var PieChart = function($title) {
      this.chart = {
        options: {
          chart: {
            plotBackgroundColor: null,
            plotBorderWidth: null,
            plotShadow: false,
            backgroundColor: 'rgba(255,255,255,0.002)',
            style: {
              color: '#eeeeee'
            },
            marginLeft: 0
          },
          title: {
            text: $title,
            style: {'color': '#eeeeee'}
          },
          tooltip: {
            pointFormat: '<b>{point.y}</b>'
          },
          legend: {
            itemStyle:{'color':'#eeeeee'},
            itemHoverStyle:{'color':'#ffffff'},
            layout: 'vertical',
            useHTML: true,
            labelFormatter: function() {
                return '<div style="width:150px"><span style="float:left">' + this.name + '</span><span style="float:right">' + this.y + '</span></div>';
            },
          },
          plotOptions: {
            pie: {
              allowPointSelect: true,
              cursor: 'pointer',
              dataLabels: {
                enabled: false
              },
              showInLegend: true
            },
            bar: {
              dataLabels: { enabled: false },
              showInLegend: true
            }
          },
        },
        size: {
          width: "250",
          height: "400"
        },
        series: [{
          type: 'pie',
          name: 'Localities',
          data: [
          ]
        }]
      };
    };
    return PieChart;
  }
)

mandelbrotApp.factory('Mandelbrot',
  function($http, PieChart) {
    var Mandelbrot = function() {
      this.hits = [];
      this.busy = false;
      this.kw = "";
      this.city = "";
      this.area = "";
      this.fromkm = 0;
      this.tokm = 20; 
      this.from = 0;
      this.catsChartConfig = new PieChart("Top Categories");
      this.catsChartConfig.chart.options.legend.labelFormatter = function() {
        return '<div style="width:150px"><span style="float:left; font-size: 9px;">' + this.name + '</span><span style="float:right; font-size: 10px;">' + this.y + '</span></div>';
      };
      this.center = {
        lat: 0,
        lng: 0,
        zoom: 13
      };
      this.markers = {
        center: {
          lat: this.center.lat,
          lng: this.center.lng,
          focus: true,
          draggable: false
        }
      }
      this.defaults = {
        scrollWheelZoom: false
      };
      this.areaChartConfig = new PieChart("Top Localities");
      this.geoChartConfig = new PieChart("Distance Ranges");
    };

    Mandelbrot.prototype.search = function($button) {
      if(this.busy || this.from > this.count) return;
      this.busy = true;

      $http.jsonp('http://138.91.34.100:9999/search/askme/place?callback=JSON_CALLBACK', {
          'params': {
            'kw': this.kw,
            'city': this.city,
            'area': this.area,
            'lat': this.center.lat,
            'lon': this.center.lng,
            'fromkm': this.fromkm,
            'tokm': this.tokm, 
            'offset': this.from,
            'source': 'true',
            'select': 'LatLong,Address,PinCode,Area,City,State,LocationName,CompanyName,CompanyLogoURL,CompanyDescription,CustomerType,NowCustomerType,ContactLandline,ContactMobile'
          }
        }).success(function(data,status,headers,config) {
          var hits = data.results.hits.hits;
          if($button) {
            this.hits = [];
          }
          for (var i = 0; i < hits.length; i++) {
            this.hits.push(hits[i]);
          }
          if("geotarget" in data.results.aggregations) {
            for (var i = 0; i < hits.length; i++) {
              this.markers[hits[i]._source.PlaceID] = {
                lat: hits[i]._source.LatLong.lat,
                lng: hits[i]._source.LatLong.lon,
                draggable: false
              }
            }
          }
          if($button) {
            this.timems = data['server-time-ms'];
            this.slug = data.slug;
            this.count = data.results.hits.total;
            this.maxscore = data.results.hits.max_score;
            this.aggr_area = data.results.aggregations.area.buckets;
            this.aggr_cats = data.results.aggregations.products.categories.buckets;
            this.aggr_attr = data.results.aggregations.products.attributes.questions.buckets;
            this.aggr_geo = undefined;
            var catsData = [];
            for(var i=0; i<this.aggr_cats.length; ++i) {
              catsData.push([this.aggr_cats[i].key, this.aggr_cats[i].doc_count]);
            }
            this.catsChartConfig.chart.series[0].data = catsData;
            var areaData = [];
            for(var i=0; i<this.aggr_area.length; ++i) {
              areaData.push([this.aggr_area[i].key, this.aggr_area[i].doc_count]);
            }
            this.areaChartConfig.chart.series[0].data = areaData;
            if("geotarget" in data.results.aggregations) {
              this.aggr_geo = data.results.aggregations.geotarget.buckets;
              var geoData = [];
              for(var i=0; i<this.aggr_geo.length; ++i) {
                geoData.push([this.aggr_geo[i].key, this.aggr_geo[i].doc_count]);
              }
              this.geoChartConfig.chart.series[0].data = geoData;
            }
          }
          this.from = this.from + 20;
          this.busy = false;
        }.bind(this)).error(function(data,status,headers,config) {
          this.from = 0;
          this.busy = false;
          console.log(status);
        }.bind(this));

    };
    return Mandelbrot;
  }
);

