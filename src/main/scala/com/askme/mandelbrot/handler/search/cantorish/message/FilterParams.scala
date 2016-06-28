package com.askme.mandelbrot.handler.search.cantorish.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class FilterParams(category: String, base_id: Int, variant_id: Int, subscribed_id: Int,
                        store_id: Int, city: String, seller_id: Int, brand: String) extends RestMessage
