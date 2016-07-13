package com.askme.mandelbrot.handler.search.grocery.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class FilterParams(category: String, variant_id: Int, product_id: Int, item_id: String,
                        storefront_id: Int, geo_id: Long, zone_code: String, brand: String,
                        user_id: String, order_id: String, parent_order_id: String, order_status: String,
                        order_updated_since: String, order_geo_id: Long, include_inactive_items: Boolean) extends RestMessage
