package com.askme.mandelbrot.util

import scala.util.matching.Regex

/**
 * Created by adichad on 18/04/15.
 */
object Utils {

  implicit class `string utils`(val s: String) extends AnyVal {
    def nonEmptyOrElse(other: => String) = if (s.isEmpty) other else s

    def tokenize(regex: Regex): List[List[String]] = regex.findAllIn(s).matchData.map(_.subgroups).toList

    val specials: Seq[(String, String)] = ("\\", "\\\\") +: (("\"", "\\\"") +: (0x0000.toChar.toString, "") +:
      (for (c <- (0x0001 to 0x001F)) yield (c.toChar.toString, "\\u" + ("%4s" format Integer.toHexString(c)).replace(" ", "0"))))

    def escapeJson: String = {

      var res = s
      specials.foreach {
        c => res = res.replace(c._1, c._2)
      }
      res
    }
  }


}
