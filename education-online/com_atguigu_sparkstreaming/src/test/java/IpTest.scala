import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object IpTest {
  def main(args: Array[String]): Unit = {
    val ipSearch = new DbSearcher(new DbConfig(), this.getClass.getResource("/ip2region.db").getPath)
    val region = ipSearch.binarySearch("182.250.250.42").getRegion
    println(region)
    val city = region.split("\\|")(2)
    println(city)

  }
}
