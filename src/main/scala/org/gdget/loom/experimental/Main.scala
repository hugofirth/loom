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
import org.gdget.loom.experimental.Experiment._
import org.gdget.loom.experimental.ProvGen.{Activity, Agent, Entity, Vertex => ProvGenVertex}
import org.gdget.partitioned._
import org.gdget.partitioned.data._
import org.gdget.std.all._

import scala.annotation.tailrec
import scala.collection.{mutable, Map => AbsMap}
import scala.concurrent.ExecutionContext
import scala.language.higherKinds

/** Entry point for the Loom experiments
  *
  * @author hugofirth
  */
object Main {

  /** Type Aliases for clarity */
  type ParseError = String
  type AdjListBuilder[V] = mutable.Map[V, (PartId, Map[V, Set[Unit]], Map[V, Set[Unit]])]
  import LogicalParGraph._

  private[experimental] final case class Config(dfs: String, bfs: String, rand: String, stoch: String, numK: Int,
                                                size: Int)


  def main(args: Array[String]): Unit = {

    //TEST
    val base = "/Users/hugofirth/Desktop/"
    val conf = Config(dfs = base + "provgen_dfs.json", bfs = base + "provgen_bfs.json",
      rand = base + "provgen_rand_1000.json", stoch = "", numK = 8, size = 500012)
    provGenExperiment(conf)
    // /TEST


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
  private final def nStreamToAdj[V: Partitioned, E[_]: Edge, P]
    (ns: Stream[UNeighbourhood[V, E]], adjBldr: AdjListBuilder[V], p: P)
    (implicit pEv: Partitioner[P, (AdjListBuilder[V], UNeighbourhood[V, E])]): AdjListBuilder[V] = ns match {
      case hd #:: tl =>
        //add hd to adj creating dAdj
        //TODO: Clean up typeclass style to be consistent, i.e. Partitioner[P].partition, or switch to machinist
        val (dP, hdPart) = pEv.partition(p, (adjBldr, hd))
        val dAdj =  adjBldr += (hd.center -> (hdPart.getOrElse(0.part), hd.in, hd.out))
        nStreamToAdj(tl, dAdj, dP)
      case _ =>
        //If neighbourhood stream is empty, just return the adj which we already have
        adjBldr
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

        //Use Partitioner[LDG].partition to fold over the stream, accumulating partitioned neighbourhoods with their new
        // partitions to the adjacency matrix at each step.
        // This will produce a LogicalParGraph and exhaust the stream
        val bldr = mutable.Map.empty[ProvGenVertex, (PartId, Map[ProvGenVertex, Set[Unit]], Map[ProvGenVertex, Set[Unit]])]
        //TODO: Clean up Partitioner typeclass
        val adj = nStreamToAdj(neighbours, bldr, p)
        val g = LogicalParGraph.fromAdjList[ProvGenVertex, HPair](adj.toMap)

        //Create experiment instance with produced graph
        val exp = ProvGenExperiment(g)

        //Run the experiment
        val results = exp.run(10, exp.periodicQueryStream(12738419))

        //Results of experiment are futures, look at our choice of return type and think about this.
        import ExecutionContext.Implicits.global
        results.onSuccess {
          case Result(t, ipt) => println(s"A ${conf.numK}-way partitioning of the ProvGen graph, generated using " +
            s"LDGPartitioner, suffered $ipt when executing its workload over $t seconds")
        }

      })

    }
    //Once we have done this for all combos, return string.

    //May need another method for Loom to easily manage multiple window sizes in experiments


    "This method is unfinished"
  }



}
