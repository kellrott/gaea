package gaea.lib

import org.apache.commons.configuration.BaseConfiguration
import com.thinkaurelius.titan.core.{TitanFactory, TitanGraph}

class GraphConnection(hostname: String) {
  val config = new BaseConfiguration()
  config.setProperty("storage.backend", "cassandra")
  config.setProperty("storage.hostname", hostname)

  def connect(): GaeaGraph = {
    new GaeaGraph(TitanFactory.open(config))
  }
}