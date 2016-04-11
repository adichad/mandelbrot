package com.askme.mandelbrot.handler.search.grocery.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class FilterParams(category: String, variant_id: Int, product_id: Int, item_id: Int,
                        zone_code: String, brand: String) extends RestMessage
