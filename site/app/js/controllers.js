'use strict';

/* Controllers */

var mandelbrotControllers = angular.module('mandelbrotControllers', []);

mandelbrotControllers.controller('HitListCtrl', ['$scope', 'leafletData', 'Mandelbrot',
  function($scope, leafletData, Mandelbrot) {
    $scope.mandelbrot = new Mandelbrot(leafletData);
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

mandelbrotApp.factory('Mandelbrot', ['$http', 'PieChart', 
  function($http, PieChart) {
    var Mandelbrot = function(leafletData) {
      this.leafletData = leafletData;
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
      this.toCircle={};
      this.fromCircle={};
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
            'select': 'LocationName,CompanyName,CompanyLogoURL,CompanyDescription,Cust',
            'sort': '_score,CustomerType.DESC'
          }
        }).success(function(data,status,headers,config) {
          var hits = data.results.hits.hits;
          if($button) {
            this.hits = [];
            this.markers = {};
          }
          for (var i = 0; i < hits.length; i++) {
            this.hits.push(hits[i]);
          }
          if("geotarget" in data.results.aggregations) {
            var center = this.center;
            var fromkm = this.fromkm;
            var tokm = this.tokm;
            this.leafletData.getMap('map').then(function(map, $center, $fromkm, $tokm) {
              var options = { fill: false, weight: 2 }
              var optionsRed = { fill: false, weight: 2, color: '#ee0000' }
              L.circle([center.lat, center.lng], 1500, options).addTo(map);
              L.circle([center.lat, center.lng], 4000, options).addTo(map);
              L.circle([center.lat, center.lng], 8000, options).addTo(map);
              L.circle([center.lat, center.lng], 30000, options).addTo(map);
              L.circle([center.lat, center.lng], fromkm*1000, optionsRed).addTo(map);
              L.circle([center.lat, center.lng], tokm*1000, optionsRed).addTo(map);
            });
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
            this.aggr_cats = data.results.aggregations.categories.buckets;
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
  }]
);

