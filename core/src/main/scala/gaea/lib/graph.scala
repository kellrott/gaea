
package gaea.lib

import java.io.{InputStream, FileInputStream}
import java.util
import org.yaml.snakeyaml.Yaml

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.commons.configuration.Configuration
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure.Graph.Variables
import org.apache.tinkerpop.gremlin.structure.{Transaction, Edge, Vertex, Graph}

import com.google.protobuf.AbstractMessage

import collection.JavaConverters._

class GraphMessage {
  var protoMessage : AbstractMessage = null
}

trait GraphRule {
  def messageIsVertex(msg : AbstractMessage)
  def generateEdges()
}

object GaeaRuleReader {
  def readFile(path: String) : Array[GraphRule] = {
    val fis = new FileInputStream(path)
    return read(fis)
  }

  def read(stream: InputStream) : Array[GraphRule] = {
    //Load YAML file into scala data structures
    var out = new ArrayBuffer[GraphRule]()
    val yaml = new Yaml()
    val o = yaml.load(stream)
    val l = o.asInstanceOf[java.util.List[java.util.Map[String,Any]]].asScala.map( _.asScala )

    l.foreach( x => {

      println(x)
    })
    return out.toArray
  }
}

class GraphSchema {
  var messages = new HashMap[String, GraphMessage]()
  var graphRules = new ArrayBuffer[GraphRule]()
}

class GaeaGraph(base: Graph) extends Graph {

  override def vertices(vertexIds: AnyRef*): util.Iterator[Vertex] = base.vertices(vertexIds)
  override def tx(): Transaction = base.tx()
  override def edges(edgeIds: AnyRef*): util.Iterator[Edge] = base.edges()
  override def variables(): Variables = base.variables()
  override def configuration(): Configuration = base.configuration()
  override def addVertex(keyValues: AnyRef*): Vertex = base.addVertex(keyValues)
  override def close(): Unit = base.close()
  override def compute[C <: GraphComputer](graphComputerClass: Class[C]): C = base.compute(graphComputerClass)
  override def compute(): GraphComputer = base.compute()


}