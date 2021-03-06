import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.Server
import com.askme.mandelbrot.util.GlobalDynamicConfiguration
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import com.typesafe.config.Config
import grizzled.slf4j.Logging
/**
  * Created by Nihal on 20/07/16.
  */

class testDynamicConfig extends FlatSpec with Configurable with BeforeAndAfterAll with Logging{
  val parentPath = ""
  var config:Config = null

  override def beforeAll() = {
    info("Starting Tests...")
    super.beforeAll()
    config = configure("test-config.conf")
    GlobalDynamicConfiguration.polledConfig = config
  }

  override def afterAll() =  {
    info("Tests Finished! Clearing memory...")
    config = null
    GlobalDynamicConfiguration.polledConfig = null
    super.afterAll()
  }

  "Config" should "read basic data types with no []" in {
    assert(int("test-age")==24)
    assert(string("test-name") == "nihal")
    assert(bytes("test-bites") == 1)
    assert(long("test-height") == 175175175175175L)
    assert(list[String]("test-gfs")==List("shiela","munni"))
    assert(double("test-waist")==30.50)
    assert(boolean("test-liar"),true)
  }

  "Config" should "read basic data types with nested objects" in {
    assert(int("test-nested[0].test-age")==24)
    assert(string("test-nested[0].test-name") == "nihal")
    assert(bytes("test-nested[0].test-bites") == 1)
    assert(long("test-nested[0].test-height") == 175175175175175L)
    assert(list[String]("test-nested[0].test-gfs")==List("shiela","munni"))
    assert(double("test-nested[0].test-waist")==30.50)
    assert(boolean("test-nested[0].test-liar"),true)
  }

  "Config" should "read custom data types " in {
//    val servers = map[Server]("server").values.toList
//    assert(servers.size==1)
//    assert(obj[Server]("server.root").isInstanceOf[Server])
    assert(conf("test-nested[0]").getString("test-name") == "nihal")
    assert(confs("test-nested").get(0).getString("test-name") == "nihal")
    assert(keys("test-nested[0]").size()==7 && keys("test-nested[0]").contains("test-name"))
    assert(vals("test-nested[0]").size()==7 && vals("test-nested[0]").contains("nihal"))
  }
  "Config" should "obviously get updated configuration" in {
    GlobalDynamicConfiguration.polledConfig = ConfigFactory.load(ConfigFactory.parseString("{}").withFallback(config))
    assert(string("test-name") == "nihal")
    GlobalDynamicConfiguration.polledConfig = ConfigFactory.load(ConfigFactory.parseString("{test-name=nihal2}").withFallback(config))
    assert(string("test-name") == "nihal2")
  }


}
