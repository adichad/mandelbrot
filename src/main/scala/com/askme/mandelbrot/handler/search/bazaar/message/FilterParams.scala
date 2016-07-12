package com.askme.mandelbrot.handler.search.bazaar.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class FilterParams(category: String, product_id: Int, grouped_id: Int, base_id: Int, subscribed_id: Int,
                        store: String, city: String, national_only: Boolean, ndd_only: Boolean,
                        store_front_id: Int, mpdm_store_front_id: Int,
                        crm_seller_id: Int, brand: String,
                        filters: Map[String, String], optionFilters: Map[String, String],
                        price_min: Float, price_max: Float, category_id: Int) extends RestMessage
