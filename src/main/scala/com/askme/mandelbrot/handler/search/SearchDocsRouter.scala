package com.askme.mandelbrot.handler.search

import com.askme.mandelbrot.handler.{MandelbrotHandler, Router}
import spray.http.MediaTypes._

/**
 * Created by adichad on 31/03/15.
 */
case object SearchDocsRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    jsonpWithParameter("callback") {
      path("apidocs" / Segment / Segment) { (index, esType) =>
        respondWithMediaType(`application/json`) {
          complete {
            s"""
             |{
             | "api": "GET /search/$index/$esType",
             | "version": "0.1.0",
             | "comments": {
             |   "1": "all parameter-values should be url-encoded",
             |   "2": { "when lat-long parameters are specified": [
             |            "results are filtered by fromkm-tokm parameters",
             |            "sorting changes to relevance within radial distance buckets: 0-1.5km, 1.5-4km, 4-8km, 8-30km, 30km+",
             |            "unless exact LocationName is specified in keywords, which are always assigned the top distance bucket (0-1.5km)"
             |          ]
             |        },
             |   "3": "each place may have one or more Products, each assigned a category and a set of attributes. The category filter works accordingly.",
             |   "4": "Product/Attribute information is retrievable in its original structure by passing source=true. the select=<field-list> parameter should be used instead when the full response is not needed",
             |   "5": "paging is implemented using offset= and size= parameters, paging beyond the 2000th record may timeout and is not recommended, recommendation is to refine your search instead"
             | },
             | "parameters": {
             |   "kw": {
             |     "type": "String",
             |     "required": false,
             |     "default": "",
             |     "description": "free-form 'keywords'/'text' searched in analyzed fields",
             |     "multivalued": false,
             |     "searched-fields": [
             |       "LocationName", "CompanyAliases", "Product.l3category", "Product.categorykeywords",
             |       "LocationType", "BusinessType", "Product.name", "Product.brand", "Area", "AreaSynonyms",
             |       "City", "CitySynonyms", "Product.stringattribute.answer"
             |     ]
             |   },
             |   "city": {
             |     "type": "String",
             |     "required": false,
             |     "default": "",
             |     "description": "filter on 'City' field",
             |     "multivalued": true,
             |     "seperator": ","
             |   },
             |   "area": {
             |     "type": "String",
             |     "required": false,
             |     "default": "",
             |     "description": "filter on 'Area', 'AreaSynonyms' fields",
             |     "multivalued": true,
             |     "seperator": ","
             |   },
             |   "pin": {
             |     "type": "String",
             |     "required": false,
             |     "default": "",
             |     "description": "filter on 'PinCode' field",
             |     "multivalued": true,
             |     "seperator": ","
             |   },
             |   "category": {
             |     "type": "String",
             |     "required": false,
             |     "default": "",
             |     "description": "filter on 'Product.l3categoryexact' field, for use in navigation from the 'categories' aggregation",
             |     "multivalued": true,
             |     "seperator": "#"
             |   },
             |   "id": {
             |     "type": "String",
             |     "required": false,
             |     "default": "",
             |     "description": "filter by document ids, use with source=true for location detail rendering",
             |     "multivalued": true,
             |     "seperator": ","
             |   },
             |   "size": {
             |     "type": "Integer",
             |     "required": false,
             |     "default": 20,
             |     "description": "the number of hits to return",
             |     "multivalued": false
             |   },
             |   "offset": {
             |     "type": "Integer",
             |     "required": false,
             |     "default": 0,
             |     "description": "the number of hits to skip from the top, used for paging in tandem with 'size'",
             |     "multivalued": false
             |   },
             |   "lat": {
             |     "type": "Double",
             |     "required": false,
             |     "default": 0.0,
             |     "description": "latitude (degrees) of point around which to focus search",
             |     "multivalued": false
             |   },
             |   "lon": {
             |     "type": "Double",
             |     "required": false,
             |     "default": 0.0,
             |     "description": "longitude (degrees) of point around which to focus search",
             |     "multivalued": false
             |   },
             |   "fromkm": {
             |     "type": "Double",
             |     "required": false,
             |     "default": 0.0,
             |     "description": "distance in km from point specified by 'lat','lon' that specifies a lower-bound (inclusive) on the distance filter; for use in navigation from the 'geotarget' aggregation",
             |     "multivalued": false
             |   },
             |   "tokm": {
             |     "type": "Double",
             |     "required": false,
             |     "default": 20.0,
             |     "description": "distance in km from point specified by 'lat','lon' that specifies an upper-bound (inclusive) on the distance filter; for use in navigation from the 'geotarget' aggregation",
             |     "multivalued": false
             |   },
             |   "select": {
             |     "type": "String",
             |     "required": false,
             |     "default": "_id",
             |     "description": "list of field values to retrieve for each hit, caveat: nested fields are returned flattened",
             |     "multivalued": true,
             |     "seperator": ","
             |   },
             |   "agg": {
             |     "type": "Boolean",
             |     "required": false,
             |     "default": true,
             |     "description": "whether to compute aggregations on the result-set",
             |     "multivalued": false
             |   },
             |   "aggbuckets": {
             |     "type": "Integer",
             |     "required": false,
             |     "default": 10,
             |     "description": "number of buckets to return for each aggregation",
             |     "multivalued": false
             |   },
             |   "source": {
             |     "type": "Boolean",
             |     "required": false,
             |     "default": false,
             |     "description": "whether to include raw _source depicting the indexed document for every result",
             |     "multivalued": false
             |   }
             | },
             | "example": {
             |   "request": "GET http://search.production.askme.com:9999/search/askme/place?kw=luxury%20spa&city=delhi&select=Area,Address,ContactLandLine,LocationLandLine,ContactMobile,LocationEmail,ContactEmail,LocationName,CompanyDescription,Product.l3category,LatLong,City&lat=28.6679&lon=77.2342&fromkm=0&tokm=20&size=2&agg=false",
             |   "response": {
             |     "slug": "/delhi/search/luxury-spa",
             |     "hit-count": 2,
             |     "server-time-ms": 290,
             |     "results": {
             |     "slug": "/delhi/search/luxury-spa",
             |     "hit-count": 2,
             |     "server-time-ms": 224,
             |     "results": {
             |       "took": 114,
             |       "timed_out": false,
             |       "terminated_early": false,
             |       "_shards": {
             |           "total": 15,
             |           "successful": 15,
             |           "failed": 0
             |       },
             |       "hits": {
             |           "total": 990,
             |           "max_score": 3068772140000000000,
             |           "hits": [
             |               {
             |                   "_index": "askme_a",
             |                   "_type": "place",
             |                   "_id": "U2083752905L34715333",
             |                   "_score": 4901054,
             |                   "fields": {
             |                       "Product.l3category": [
             |                           "Beauty Parlors & Salons- Unisex"
             |                       ],
             |                       "LatLong": [
             |                           "28.6621041316754,77.2353025979614"
             |                       ],
             |                       "LocationName": [
             |                           "Senzi Salon"
             |                       ],
             |                       "CompanyDescription": [
             |                           ""
             |                       ],
             |                       "Area": [
             |                           "Delhi GPO"
             |                       ],
             |                       "ContactEmail": [
             |                           ""
             |                       ],
             |                       "Address": [
             |                           "H-12 1st Floor Kailash Colony"
             |                       ],
             |                       "LocationEmail": [
             |                           ""
             |                       ],
             |                       "LocationLandLine": [
             |                           "41632746,41632706"
             |                       ],
             |                       "ContactMobile": [
             |                           ""
             |                       ],
             |                       "City": [
             |                           "Delhi"
             |                       ]
             |                   },
             |                   "sort": [
             |                       0,
             |                       4901054
             |                   ]
             |               },
             |               {
             |                   "_index": "askme_a",
             |                   "_type": "place",
             |                   "_id": "U3433480L6204914",
             |                   "_score": 4886159,
             |                   "fields": {
             |                       "Product.l3category": [
             |                           "Beauty Parlors & Salons- Unisex"
             |                       ],
             |                       "LatLong": [
             |                           "28.6755306,77.2232708999999"
             |                       ],
             |                       "LocationName": [
             |                           "Glimmer Unisex Saloon"
             |                       ],
             |                       "CompanyDescription": [
             |                           "We are service Provider for Hair Cutting and Saloon"
             |                       ],
             |                       "Area": [
             |                           "Civil Lines"
             |                       ],
             |                       "ContactEmail": [
             |                           ""
             |                       ],
             |                       "Address": [
             |                           "9 Ground Floor Rajpur Road"
             |                       ],
             |                       "LocationEmail": [
             |                           ""
             |                       ],
             |                       "LocationLandLine": [
             |                           ""
             |                       ],
             |                       "ContactMobile": [
             |                           "9711262333"
             |                       ],
             |                       "City": [
             |                           "Delhi"
             |                       ]
             |                   },
             |                   "sort": [
             |                       0,
             |                       4886159
             |                   ]
             |               }
             |           ]
             |         },
             |         "aggregations": {}
             |       }
             |     }
             |   }
             | },
             | "dev-contact": {
             |   "name": "adi",
             |   "email": "aditya.chadha@getitinfomedia.com",
             |   "phone": "+91 81308.02929"
             | }
             |}""".stripMargin
          }
        }
      }
    }
  }


}
