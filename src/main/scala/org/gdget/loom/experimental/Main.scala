/** loom
  *
  * Copyright (c) 2016 Hugo Firth
  * Email: <me@hugofirth.com/>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at:
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.gdget.loom.experimental

import java.nio.file.Paths

import cats._
import cats.implicits._
import jawn.ParseException
import jawn.ast.JValue
import org.gdget._
import org.gdget.data.UNeighbourhood
import org.gdget.loom.GraphReader
import org.gdget.loom.experimental.ProvGen.{Activity, Agent, Entity, Vertex => ProvGenVertex}
import org.gdget.partitioned.{PartId, Partitioned, Partitioner}
import org.gdget.partitioned.data._
import org.gdget.std.all._

import scala.annotation.tailrec
import scala.language.higherKinds

/** Entry point for the Loom experiments
  *
  * @author hugofirth
  */
object Main {

  /** Type Aliases for clarity */
  type ParseError = String
  import LogicalParGraph._

  private[experimental] final case class Config(dfs: String, bfs: String, rand: String, stoch: String, numK: Int,
                                                size: Int)


  def main(args: Array[String]): Unit = {



  }

  /** Method for parsing a JSON value to a given Vertex ADT */
  def jsonToNeighbourhood[V: Partitioned](jValue: JValue,
                                          eToV: (String, Int, Direction) => Either[ParseError, V],
                                          vToV: (String, Int) => Either[ParseError, V]): Either[String, UNeighbourhood[V, HPair]] = {

    def getId(j: JValue) = {
      val idOpt = j.get("id").getLong
      idOpt.fold(Either.left[String, Long]("Unable to properly parse vertex id"))(Either.right[String, Long])
    }

    def getCenter(j: JValue, id: Int) = {
      val lblOpt = j.get("label").getString
      lblOpt.fold(Either.left[String, V]("Unable to properly parse vertex label"))(vToV(_:String, id))
    }

    def getNeighbour(j: JValue, d: Direction) = {
      val neighbourId = getId(j)
      neighbourId.flatMap {id =>
        val lblOpt = j.get("label").getString
        lblOpt.fold(Either.left[String, V]("Unable to properly parse edge label"))(eToV(_:String, id.toInt, d))
      }
    }

    def getNeighbours(j: JValue, d: Direction) = {
      Stream.from(0).map(j.get).takeWhile(_.nonNull).map(getNeighbour(_, d)).sequence.map(_.toList)
    }

    //Format of each entry in Json array: {id, label, in[{neighbourId, edgeLabel}, ...], out[{}, ...]}

    //TODO: Why are we using Set of unit rather than empty Set here?
    for {
      id <- getId(jValue)
      center <- getCenter(jValue, id.toInt)
      inN <- getNeighbours(jValue.get("in"), In)
      outN <- getNeighbours(jValue.get("out"), Out)
      edges = Set(())
    } yield UNeighbourhood[V, HPair](center, inN.map(_ -> edges).toMap, outN.map(_ -> edges).toMap)

  }

  /**  Tail recursive method to consume Neighbourhood stream and produce a LogicalParGraph */
  @tailrec
  def nStreamToGraph[V: Partitioned, E[_]: Edge](ns: Stream[UNeighbourhood[V, E]],
                                                                 g: LogicalParGraph[V, E]): LogicalParGraph[V, E] = ns match {
    case hd #:: tl =>
      //add hd to g creating dG
      val dG = g
      nStreamToGraph(tl, dG)
    case _ =>
      //If neighbourhood stream is empty, just return the g which we already have
      g
  }

  /** Method describes the setup and execution of the ProvGen Loom experiment */
  def provGenExperiment(conf: Config): String = {

    val vToV: (String, Int) => Either[String, ProvGenVertex] = {
      case ("AGENT", id) =>
        Right(Agent(id, None))
      case ("ACTIVITY", id) =>
        Right(Activity(id, None))
      case ("ENTITY", id) =>
        Right(Entity(id, None))
      case other =>
        Left(s"Unrecognised vertex label $other")
    }

    val eToV: (String, Int, Direction) => Either[String, ProvGenVertex] = {
      case ("WASDERIVEDFROM", id, _) =>
        Right(Entity(id.toInt, None))
      case ("WASGENERATEDBY", id, d) =>
        Right(if(d == Out) Entity(id.toInt, None) else Activity(id.toInt, None))
      case ("WASASSOCIATEDWITH", id, d) =>
        Right(if(d == Out) Activity(id.toInt, None) else Agent(id.toInt, None))
      case ("USED", id, d) =>
        Right(if(d == Out) Activity(id.toInt, None) else Entity(id.toInt, None))
      case other =>
        Left(s"Unrecognised edge label $other")
    }

    //For each stream order
    List(conf.dfs, conf.bfs, conf.rand).map { order =>
      //Get json dump = - create Stream[UNeighbourhood[ProvGenVertex, HPair]] with GraphReader
      val nStream = GraphReader.read(order, jsonToNeighbourhood[ProvGenVertex](_: JValue, eToV, vToV))
      //Fold in order to deal with possible parsing errors
      nStream.fold({ e =>
        val error = s"Error parsing json file $order on line ${e.line}: ${e.msg}"
        System.err.println(error)
        error
      }, {neighbours =>

        //Given Stream of neighbourhoods, for each partitioner
        import Partitioners._

        //Create LDG partitioner for LogicalParGraph, ProvGenVertex, HPair, which means we need to know the final size
        // of the graph (conf value) as well as k (number of partitions)
        val p = LDGPartitioner(conf.size/conf.numK, Map.empty[PartId, Int], conf.numK)

        //Create an empty LogicalParGraph
        val g = LogicalParGraph.empty[ProvGenVertex, HPair]

        //Use Partitioner[LDG].partition to fold over the stream, accumulating partitioned neighbourhoods with their new
        // partitions to the graph at each step. This step will be very performance intensive, look at how to handle.
        // For one, maybe it should be a tail recursive functinon instead of a fold?
        // We need a mutable/builder based approach to making graphs
        // This will produce a LogicalParGraph and exhaust the stream






        //Create experiment insance with produced graph

        //Run the experiment

        //Results of experiment are futures, look at our choice of return type and think about this.

      })

    }

      //Get json dump -

      //In order to to do ^^ we will need to define a function which takes a JValue for each elem of the adjList JSon
      // and returns parses it to return a Uneighbourhood

      //Given Stream[Neighbourhood], for each partitioner:

        //Consume Stream[Neighbourhood] to produce g: LogicalParGraph[ProvGenVertex, HPair]

        //Now that we have g, create ProvGenExperiment instance and run 100 queries with one of the two stream options

        //Note that ^^ requires that we have defined the ProvGen appropriate queries in ProvGenExpMeta

        //Once we have the results (number of traversals and time), log it to console and add to Result string

    //Once we have done this for all combos, return string.

    //May need another method for Loom to easily manage multiple window sizes in experiments


    ???
  }



}
