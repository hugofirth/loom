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
import java.util.Calendar

import cats._
import cats.implicits._
import jawn.ParseException
import jawn.ast.JValue
import org.gdget._
import org.gdget.data.{SimpleGraph, UNeighbourhood}
import org.gdget.loom._
import org.gdget.loom.experimental.Experiment._
import org.gdget.loom.experimental.ProvGen.{Activity, Agent, Entity, Vertex => ProvGenVertex}
import org.gdget.partitioned._
import org.gdget.partitioned.data._
import org.gdget.std.all._

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.collection.{mutable, Map => AbsMap}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.language.higherKinds
import scala.util.Random

/** Entry point for the Loom experiments
  *
  * @author hugofirth
  */
object Main {

  /** Type Aliases for clarity */
  type ParseError = String
  type AdjListBuilder[V] = mutable.Map[V, (PartId, Map[V, Set[Unit]], Map[V, Set[Unit]])]
  import LogicalParGraph._

  val qSeed = 12738419

  private[experimental] final case class Config(dfs: String, bfs: String, rand: String, stoch: String, numK: Int,
                                                size: Int, prime: Int)

  def main(args: Array[String]): Unit = {

    //TEST
    val base = "/Users/hugofirth/Desktop/Data/Loom/provgen/"
    val conf = Config(dfs = base + "provgen_dfs.json", bfs = base + "provgen_bfs.json",
      rand = base + "provgen_rand_1000.json", stoch = "", numK = 8, size = 500012, prime = 251)
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
        val lblOpt = j.get("rel").getString
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
      inN <- getNeighbours(jValue.get("in"), Out)
      outN <- getNeighbours(jValue.get("out"), In)
      edges = Set(())
    } yield UNeighbourhood[V, HPair](center, inN.map(_ -> edges).toMap, outN.map(_ -> edges).toMap)

  }

  /**  Tail recursive method to consume Neighbourhood stream and produce a LogicalParGraph */
  @tailrec
  private final def nStreamToAdj[V: Partitioned, E[_]: Edge, P]
    (ns: Stream[UNeighbourhood[V, E]], adjBldr: AdjListBuilder[V], p: P)
    (implicit pEv: Partitioner[P, UNeighbourhood[V, E], AdjListBuilder[V], Option]): AdjListBuilder[V] = ns match {
      case hd #:: tl =>
        //add hd to adj creating dAdj
        //TODO: Clean up typeclass style to be consistent, i.e. Partitioner[P].partition, or switch to machinist
        val (dP, partitioned) = pEv.partition(p, hd, adjBldr)
        val dAdj = partitioned.fold(adjBldr) { case (dHd, dHdPart) =>
          adjBldr += (dHd.center -> (dHdPart, dHd.in, dHd.out))
        }
        //TODO: Make sure that partitioner actually updates hd.center to contain this partition (unless we're sticking to
        //  the other wrapper style). Otherwise could have unforseen consequences.
        nStreamToAdj(tl, dAdj, dP)
      case _ =>
        //If neighbourhood stream is empty, just return the adj which we already have
        adjBldr
    }

  /** Tail recursive method to consume an edge stream and produce a LogicalParGraph */
  @tailrec
  private final def eStreamToAdj[V: Partitioned, E[_]: Edge, P](es: Stream[E[V]], adjBldr: AdjListBuilder[V], p: P)
                                                               (implicit pEv: Partitioner[P, E[V], AdjListBuilder[V], List]): AdjListBuilder[V] = {
    //Match over edge stream
    es match {
      case hd #:: tl =>
        //Submit edge to be partitioned and accept a list of already partitioned edges (as we expect this partitioner to
        //  be windowed. Should probably represent that explicitly in types ...
        val (dP, partitioned) = pEv.partition(p, hd, adjBldr)
        //Add partitioned edges to adjBldr
        val dAdj = partitioned.foldLeft(adjBldr) { (adj, p) =>
          //Get the edge's constituent vertices
          val (e, ePart) = p
          val (l,r) = Edge[E].vertices(e)
          //For each vertex, find if they exist
          //If a vertex already exists, merely add the other vertex as a neighbour, the r vertex is an out neighbour of
          // the l vertex and visa versa
          val lN = adj.get(l).fold((ePart, Map.empty[V, Set[Unit]], Map(r -> Set(())))) { case (pId, in, out) =>
            (pId, in, out + (r -> Set(())))
          }
          val rN = adj.get(r).fold((ePart, Map(l -> Set(())), Map.empty[V, Set[Unit]])) { case (pId, in, out) =>
            (pId, in + (l -> Set(())), out)
          }
          //Update the adjBldr - mutability I know :|
          adj.update(l, lN)
          adj.update(r, rN)
          adj
        }
        //Once we have updated the adjBldr for this round of assignments, move on to the next edge in the stream
        eStreamToAdj(tl, dAdj, dP)
      case _ =>
        //If the edge stream is empty, just return the adj which we have built so far
        adjBldr
    }

  }

  /** Method describes the setup and execution of the ProvGen Loom experiment */
  def provGenExperiment(conf: Config): String = {

    //Create the Labelled Instance for ProvGenVertex. Doing it here means we have access to the prime value passed in
    //  conf, but it still feels horrible. This whole file needs to be sorted, split out etc...
    implicit val pgVLabelled = new Labelled[ProvGenVertex] {

      val labels = Random.shuffle((1 until conf.prime).toVector).take(3)

      override def label(a: ProvGenVertex): Int = a match {
        case Entity(_, _) => labels(0)
        case Agent(_, _) => labels(1)
        case Activity(_, _) => labels(2)
      }
    }

    def time = Calendar.getInstance.getTime.toString

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

    println(s"Start reading in json @ $time")

    def altPartitioning(neighbours: Stream[UNeighbourhood[ProvGenVertex, HPair]]) = {

      //Given Stream of neighbourhoods, for each partitioner
      import Partitioners._

      println(s"Create the partitioner @ $time")

      //Create LDG partitioner for LogicalParGraph, ProvGenVertex, HPair, which means we need to know the final size
      // of the graph (conf value) as well as k (number of partitions)
      val p = LDGPartitioner(conf.size/conf.numK, Map.empty[PartId, Int], conf.numK)
      //        val p = HashPartitioner(conf.numK, 0.part)
      println(s"Start parsing json in to graph @ $time")

      //Use Partitioner[LDG].partition to fold over the stream, accumulating partitioned neighbourhoods with their new
      // partitions to the adjacency matrix at each step.
      // This will produce a LogicalParGraph and exhaust the stream
      val bldr = mutable.Map.empty[ProvGenVertex, (PartId, Map[ProvGenVertex, Set[Unit]], Map[ProvGenVertex, Set[Unit]])]
      //TODO: Clean up Partitioner typeclass
      val adj = nStreamToAdj(neighbours, bldr, p)
      LogicalParGraph.fromAdjList[ProvGenVertex, HPair](adj.toMap)
    }

    def loomPartitioning(edges: Stream[HPair[ProvGenVertex]]) = {

      println(s"Create the partitioner @ $time")

      //Bit of a hack (ok a lot of a hack)
      //We need the motifs before we can create the graph so we need to create the experiment with an empty graph and then
      // create a query stream with the same seed in this method and in the nStream fold
      //TODO: Make this a little more principled by making many of the methods on Experiment static

      //TODO: Pull G out of top level TPSTry/Node definition. Its not needed.
      val hackExp = ProvGenExperiment(LogicalParGraph.empty[ProvGenVertex, HPair])
      val qStream = hackExp.fixedQueryStream(qSeed, Map("q1" -> 0.5, "q2" -> 0.5))
      val trie = qStream.take(30).map(_._2).foldLeft(TPSTry.empty[ProvGenVertex, HPair](conf.prime)) { (trie, g) =>
        trie.add(g)
      }
      val motifs = trie.motifsFor(0.5)

      //Create the Loom partitioner for LogicalParGraph, ProvGenVertex, HPair
      val p = Loom[LogicalParGraph, ProvGenVertex, HPair](conf.size/conf.numK, Map.empty[PartId, Int], conf.numK,
        motifs, Map.empty[ProvGenVertex, Set[(Set[HPair[ProvGenVertex]], TPSTryNode[ProvGenVertex, HPair])]],
        Queue.empty[HPair[ProvGenVertex]], 30000, 2, conf.prime)


      val bldr = mutable.Map.empty[ProvGenVertex, (PartId, Map[ProvGenVertex, Set[Unit]], Map[ProvGenVertex, Set[Unit]])]
      val adj = eStreamToAdj(edges, bldr, p)
      LogicalParGraph.fromAdjList[ProvGenVertex, HPair](adj.toMap)
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


        //TODO: Fix bug in TPSTry
//        val g = altPartitioning(neighbours)
        val g = loomPartitioning(neighbours.flatMap(_.inEdges))

        val pSizes = g.partitions.map(_.size).mkString("(", ", ", ")")
        println(s"Partition sizes are: $pSizes")

        println(s"Finished parsing json in to graph @ $time")

        println(s"Create experiment @ $time")

        //Create experiment instance with produced graph
        val exp = ProvGenExperiment(g)

        println(s"Start running experiment @ $time")

        //Test queries
        exp.trial()

        //Run the experiment
        val results = exp.run(10, exp.fixedQueryStream(qSeed, Map("q1" -> 0.7, "q2" -> 0.3)))

        println(s"Finish running experiment @ $time")

        //Results of experiment are futures, look at our choice of return type and think about this.
        import ExecutionContext.Implicits.global
        results.onSuccess {
          case Result(t, ipt) => println(s"A ${conf.numK}-way partitioning of the ProvGen graph, generated using " +
            s"Loom, suffered $ipt when executing its workload over $t seconds")
        }

        Await.result(results, Duration.Inf)

      })

    }
    //Once we have done this for all combos, return string.

    //May need another method for Loom to easily manage multiple window sizes in experiments


    "This method is unfinished"
  }



}
