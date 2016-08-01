package com.askme.mandelbrot.handler.search.cantorish.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class FilterParams(category: String, base_id: Int, variant_id: Int, subscribed_id: String,
                        city: String, seller_id: Int, brand: String,
                        base_active_only: Boolean, variant_active_only: Boolean,
                        subscription_active_only: Boolean, seller_active_only: Boolean) extends RestMessage
