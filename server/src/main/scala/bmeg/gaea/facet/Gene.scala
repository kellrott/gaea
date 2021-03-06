package bmeg.gaea.facet

import bmeg.gaea.titan.Titan
import bmeg.gaea.schema.Variant
import bmeg.gaea.convoy.Ingest
import bmeg.gaea.feature.Feature

import org.http4s._
import org.http4s.server._
import org.http4s.dsl._

import com.thinkaurelius.titan.core.TitanGraph
import gremlin.scala._

import _root_.argonaut._, Argonaut._
import org.http4s.argonaut._
import scalaz.stream.text
import scalaz.stream.Process
import scalaz.stream.Process._
import scalaz.stream.Process1
import scalaz.concurrent.Task

object GeneFacet {
  val graph = Titan.connect(Titan.configuration(Map[String, String]()))

  def splitLines(rest: String): Process1[String, String] =
    rest.split("""\r\n|\n|\r""", 2) match {
      case Array(head, tail) =>
        emit(head) ++ splitLines(tail)
      case Array(head) =>
        receive1Or[String, String](emit(rest)) { s => splitLines(rest + s) }
    }

  def puts(line: String): Task[Unit] = Task { println(line) }

  def commit(graph: TitanGraph): Process[Task, Unit] = Process eval_ (Task {
    graph.tx.commit()
  })

  def retryCommit(graph: TitanGraph) (times: Integer): Unit = {
    if (times == 0) {
      println("TRANSACTION FAILED!")
    } else {
      println("trying transaction: " + times.toString)
      try {
        graph.tx.commit()
      } catch {
        case ex: Exception => {
          retryCommit(graph) (times - 1)
        }
      }
    }
  }

  val service = HttpService {
    case GET -> Root / "hello" / name =>
      Ok(jSingleObject("message", jString(s"Hello, ${name}")))

    case GET -> Root / "gene" / name =>
      val graph = Titan.connect(Titan.configuration(Map[String, String]()))
      val synonym = Feature.findSynonym(graph) (name).getOrElse {
        "no synonym found"
      }
      Ok(jSingleObject(name, jString(synonym)))

    case request @ POST -> Root / "message" / messageType =>
      val messages = request.bodyAsText.pipe(text.lines(1024 * 1024 * 64)).flatMap { line =>
        Process eval Ingest.ingestMessage(messageType) (graph) (line)
      } 
      messages.runLog.run
      retryCommit(graph) (5)

      Ok(jString("done!"))

    case request @ POST -> Root / "yellow" =>
      val y = request.bodyAsText.pipe(text.lines()).flatMap { line =>
        Process eval puts(line)
      }
      y.runLog.run
      Ok(jNumber(1))
  }
}

