package com.askme.mandelbrot.loader

import java.io.{BufferedOutputStream, FileOutputStream, BufferedInputStream, FileInputStream}
import java.nio.charset.Charset
import java.util.zip.GZIPInputStream
import com.askme.mandelbrot.Configurable
import grizzled.slf4j.Logging

import scala.io.{Codec, Source}

/**
 * Created by adichad on 04/11/14.
 */


object CSVLoader extends App with Logging with Configurable {
  implicit class `nonempty or else`(val s: String) extends AnyVal {
    def nonEmptyOrElse(other: => String) = if (s.trim.isEmpty) other else s }

  protected[this] val config = configure("environment", "application", "environment_defaults", "application_defaults")
  try {
    backFillSystemProperties("component.name", "log.path.current", "log.path.archive", "log.level")
    val input =
      new GZIPInputStream(new BufferedInputStream(new FileInputStream(string("file.input"))))
    val output = new BufferedOutputStream(new FileOutputStream(string("file.output")))
    val sb = (new StringBuilder)
    Source.fromInputStream(input)(Codec.ISO8859).getLines().foreach(
      line => {
        val cells = line.replace(0.toChar.toString, "").split(9.toChar.toString, -1)

        sb.append("{ \"index\" : { \"_index\" : \"askme\", \"_type\" : \"place\", \"_id\" : \"").append(cells(1)).append("\" } }\n")
        sb.append("{ ")
        sb.append("\"").append("UniqueID").append("\": \"").append(cells(1)).append("\"")
        sb.append(", ").append("\"").append("BusinessUserID").append("\": ").append(cells(2))
        sb.append(", ").append("\"").append("ProductID").append("\": ").append(cells(3))
        sb.append(", ").append("\"").append("LocationID").append("\": ").append(cells(4))
        sb.append(", ").append("\"").append("BusinessCompanyID").append("\": ").append(cells(5))
        sb.append(", ").append("\"").append("L3CategoryID").append("\": ").append(cells(6))
        sb.append(", ").append("\"").append("LastUpdated").append("\": \"").append(cells(12)).append("\"")
        sb.append(", ").append("\"").append("LocationName").append("\": \"").append(cells(13)).append("\"")
        sb.append(", ").append("\"").append("LocationDescription").append("\": \"").append(cells(14)).append("\"")
        sb.append(", ").append("\"").append("LocationType").append("\": \"").append(cells(15)).append("\"")
        sb.append(", ").append("\"").append("BusinessType").append("\": \"").append(cells(16)).append("\"")
        sb.append(", ").append("\"").append("Address").append("\": \"").append(cells(17)).append("\"")
        sb.append(", ").append("\"").append("Area").append("\": \"").append(cells(18)).append("\"")
        sb.append(", ").append("\"").append("AreaSynonyms").append("\": \"").append(cells(19)).append("\"")
        sb.append(", ").append("\"").append("City").append("\": \"").append(cells(20)).append("\"")
        sb.append(", ").append("\"").append("PinCode").append("\": \"").append(cells(21)).append("\"")
        sb.append(", ").append("\"").append("ZonesServed").append("\": \"").append(cells(22)).append("\"")
        sb.append(", ").append("\"").append("Zone").append("\": \"").append(cells(23)).append("\"")
        sb.append(", ").append("\"").append("State").append("\": \"").append(cells(24)).append("\"")
        sb.append(", ").append("\"").append("Country").append("\": \"").append(cells(25)).append("\"")
        sb.append(", ").append("\"").append("LatLong").append("\": { \"lat\": ").append(cells(26))
        sb.append(", ").append("\"").append("lon").append("\": ").append(cells(27)).append(" }")
        sb.append(", ").append("\"").append("Company").append("\": [{ \"name\": \"").append(cells(28)).append("\"")
        sb.append(", ").append("\"").append("description").append("\": \"").append(cells(29)).append("\"")
        sb.append(", ").append("\"").append("keywords").append("\": \"").append(cells(30)).append("\"")
        sb.append(", ").append("\"").append("email").append("\": \"").append(cells(31)).append("\"")
        sb.append(", ").append("\"").append("logourl").append("\": \"").append(cells(32)).append("\" }]")
        sb.append(", ").append("\"").append("Contact").append("\": [{ \"name\": \"").append(cells(33)).append("\"")
        sb.append(", ").append("\"").append("landline").append("\": \"").append(cells(34)).append("\"")
        sb.append(", ").append("\"").append("mobile").append("\": \"").append(cells(35)).append("\" }]")
        sb.append(", ").append("\"").append("L1Category").append("\": \"").append(cells(36)).append("\"")
        sb.append(", ").append("\"").append("L2Category").append("\": \"").append(cells(37)).append("\"")
        sb.append(", ").append("\"").append("L3Category").append("\": \"").append(cells(38)).append("\"")
        sb.append(", ").append("\"").append("CategoryPath").append("\": \"").append(cells(39)).append("\"")
        sb.append(", ").append("\"").append("CategoryKeywords").append("\": \"").append(cells(40)).append("\"")
        sb.append(", ").append("\"").append("Product").append("\": [{ \"name\": \"").append(cells(61)).append("\"")
        sb.append(", ").append("\"").append("description").append("\": \"").append(cells(62)).append("\"")
        sb.append(", ").append("\"").append("brand").append("\": \"").append(cells(63)).append("\"")
        sb.append(", ").append("\"").append("imageurls").append("\": \"").append(cells(64)).append("\" }]")
        sb.append(", ").append("\"").append("StringAttribute").append("\": [{ \"question\": \"").append(cells(65)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(41)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(66)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(42)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(67)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(43)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(68)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(44)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(69)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(45)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(70)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(46)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(71)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(47)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(72)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(48)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(73)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(49)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(74)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(50)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(75)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(51)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(76)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(52)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(77)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(53)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(78)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(54)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(79)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(55)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(80)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(56)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(81)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(57)).append("\" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(82)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": \"").append(cells(58)).append("\" }]")
        sb.append(", ").append("\"").append("IntAttribute").append("\": [{ \"question\": \"").append(cells(65)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": ").append(cells(41).nonEmptyOrElse("null")).append(" }")
        sb.append(", { ").append("\"").append("question").append("\": \"").append(cells(66)).append("\"")
        sb.append(", ").append("\"").append("answer").append("\": ").append(cells(42).nonEmptyOrElse("null")).append(" }]")
        sb.append(" }\n")
        val bytes = sb.toString.getBytes(Charset.forName("UTF-8"))
        output.write(bytes, 0, bytes.length)
        sb.setLength(0)
      }

    )
    output.close()
    input.close()
  } catch {
    case e: Throwable => error(e)
      throw e
  }

}


/*
 0  ,RowID
 1  ,"UniqueID"
 2  ,"BusinessUserID"
 3  ,"ProductID"
 4  ,"LocationID"
 5  ,"BusinessCompanyID"
 6  ,"L3CategoryID"
 7  ,"UpdateFlag"
 8  ,"InsertFlag"
 9  ,"DeleteFlag"
10  ,"FastIndexStatus"
11   ,"FastIndexTimestamp"
12   ,"LastUpdated"
13   ,"LocationName"
14   ,"LocationDescription"
15   ,"LocationType"
   ,"BusinessType"
   ,"Address"
   ,"Area"
   ,"AreaSynonyms"
20   ,"City"
   ,"PinCode"
   ,"ZonesServed"
   ,"Zone"
   ,"State"
25   ,"Country"
   ,"Latitude"
   ,"Longitude"
   ,"CompanyName"
   ,"CompanyDescription"
30   ,"CompanyKeywords"
   ,"CompanyEmail"
   ,"CompanyLogo"
   ,"PrimaryContactName"
   ,"PrimaryContactLandLine"
35   ,"PrimaryContactMobile"
   ,"L1Category"
   ,"L2Category"
   ,"L3Category"
   ,"CategoryPath"
40   ,"CategoryKeywords"
   ,"A1"
   ,"A2"
   ,"A3"
   ,"A4"
45   ,"A5"
   ,"A6"
   ,"A7"
   ,"A8"
   ,"A9"
50   ,"A10"
   ,"A11"
   ,"A12"
   ,"A13"
   ,"A14"
55   ,"A15"
   ,"A16"
   ,"A17"
   ,"A18"
   ,"A19"
60   ,"A20"
   ,"ProductName"
   ,"ProductDescription"
   ,"ProductBrand"
   ,"ProductImages"
65   ,"Q1"
   ,"Q2"
   ,"Q3"
   ,"Q4"
   ,"Q5"
70   ,"Q6"
   ,"Q7"
   ,"Q8"
   ,"Q9"
   ,"Q10"
75   ,"Q11"
   ,"Q12"
   ,"Q13"
   ,"Q14"
   ,"Q15"
80   ,"Q16"
   ,"Q17"
   ,"Q18"
   ,"Q19"
84   ,"Q20"
 */