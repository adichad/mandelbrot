package com.askme.mandelbrot.handler.search.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class FilterParams(category: String, id: String, userid: Int, locid: String, pay_type: Int) extends RestMessage
case class DealFilterParams(id: String, applicableTo: String, screentype: String, category: String, featured: String, dealsource: String, pay_merchant_id: String, edms_outlet_id: Int, gll_outlet_id: Int, pay_type: Int) extends RestMessage
