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


import cats._
import cats.implicits._
import jawn.ast.JValue
import org.gdget._
import org.gdget.data.UNeighbourhood
import org.gdget.loom._
import org.gdget.loom.experimental.Experiment._
import org.gdget.loom.experimental.ProvGen.{Vertex => ProvGenVertex}
import org.gdget.loom.experimental.MusicBrainz.{Vertex => MusicBrainzVertex}
import org.gdget.partitioned._
import org.gdget.partitioned.data._
import org.gdget.std.all._
import org.gdget.loom.util._

import scala.annotation.tailrec
import scala.collection.mutable
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

  private[experimental] final case class Config[P <: Field](dfs: String, bfs: String, rand: String, stoch: String, numK: Int,
                                                numV: Int, numE: Int, prime: P)

  def main(args: Array[String]): Unit = {


//    //TEST
//    val base = "/Users/hugofirth/Desktop/Data/Loom/provgen/"
//    val conf = Config(dfs = base + "provgen_dfs.json", bfs = base + "provgen_bfs.json",
//      rand = base + "provgen_rand.json", stoch = "", numK = 8, numV = 500012, numE = 630000, prime = P._251)
//
//    //Create the Labelled Instance for ProvGenVertex. Doing it here means we have access to the prime value passed in
//    //  conf, but it still feels horrible. This whole file needs to be sorted, split out etc...
//    implicit val pgVLabelled = ProvGenVertex.pgVLabelled(conf.prime)
//
//    //Create experiment instance
//    val exp = ProvGenExperiment(qSeed)
//
//    executeExperiment(conf, exp)
//    // /TEST

    val base = "/Users/hugofirth/Desktop/Data/Loom/musicbrainz/"
    val conf = Config(dfs = base + "musicbrainz_dfs.json", bfs = base + "musicbrainz_bfs.json",
      rand = base + "musicbrainz_rand.json", stoch = "", numK = 8, numV = 4589492, numE = 7752260, prime = P._251)

    //Create the Labelled Instance for ProvGenVertex. Doing it here means we have access to the prime value passed in
    //  conf, but it still feels horrible. This whole file needs to be sorted, split out etc...
    implicit val mBVLabelled = MusicBrainzVertex.mBVLabelled(conf.prime)

    //Create experiment instance
    val exp = MusicBrainzExperiment(qSeed)

    executeExperiment(conf, exp)

  }


  /**  Tail recursive method to consume Neighbourhood stream and produce a LogicalParGraph */
  @tailrec
  private final def nStreamToAdj[V: Partitioned, E[_]: Edge, P]
    (ns: Iterator[UNeighbourhood[V, E]], adjBldr: AdjListBuilder[V], p: P)
    (implicit pEv: Partitioner[P, UNeighbourhood[V, E], AdjListBuilder[V], Option]): AdjListBuilder[V] = {

    if(ns.hasNext) {
      //TODO: Clean up typeclass style to be consistent, i.e. Partitioner[P].partition, or switch to machinist
      val (dP, partitioned) = pEv.partition(p, ns.next(), adjBldr)
      val dAdj = partitioned.fold(adjBldr) { case (dHd, dHdPart) =>
        adjBldr += (dHd.center -> (dHdPart, dHd.in, dHd.out))
      }
      //TODO: Make sure that partitioner actually updates hd.center to contain this partition (unless we're sticking to
      //  the other wrapper style). Otherwise could have unforseen consequences.
      nStreamToAdj(ns, dAdj, dP)
    } else {
      //If neighbourhood stream is empty, just return the adj which we already have
      adjBldr
    }
  }

  /** Tail recursive method to consume an edge stream and produce a LogicalParGraph */
  @tailrec
  private final def eStreamToAdj[V: Partitioned, E[_]: Edge, P](es: Iterator[E[V]], adjBldr: AdjListBuilder[V], p: P)
                                                               (implicit pEv: Partitioner[P, E[V], AdjListBuilder[V], List]): AdjListBuilder[V] = {

    if(es.hasNext) {
      //Submit edge to be partitioned and accept a list of already partitioned edges (as we expect this partitioner to
      //  be windowed. Should probably represent that explicitly in types ...
      val (dP, partitioned) = pEv.partition(p, es.next(), adjBldr)

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
      eStreamToAdj(es, dAdj, dP)
    } else {
      //If the edge stream is empty, just return the adj which we have built so far
      adjBldr
    }

  }


  /** Method describes the setup and execution of a Loom experiment */
  def executeExperiment[P <: Field, V: Partitioned : Labelled: Parsable](conf: Config[P], exp: Experiment[V, HPair]): String = {

    import Experiment._


    println(s"Start reading in json @ $timeNow")

    //By name parameter to avoid GC unfriendly stream references
    def altPartitioning(neighbours: Iterator[UNeighbourhood[V, HPair]]) = {

      //Given Stream of neighbourhoods, for each partitioner
      import Partitioners._

      println(s"Create the partitioner @ $timeNow")

      //Create LDG partitioner for LogicalParGraph, ProvGenVertex, HPair, which means we need to know the final size
      // of the graph (conf value) as well as k (number of partitions)
//      val p = LDGPartitioner(conf.numV/conf.numK, Map.empty[PartId, Int], conf.numK)
//      val p = HashPartitioner(conf.numK, 0.part)
      val p = FennelPartitioner(Map.empty[PartId, Int], conf.numK, conf.numV, conf.numE)
      println(s"Start parsing json in to graph @ $timeNow")

      //Use Partitioner[LDG].partition to fold over the stream, accumulating partitioned neighbourhoods with their new
      // partitions to the adjacency matrix at each step.
      // This will produce a LogicalParGraph and exhaust the stream
      val bldr = mutable.Map.empty[V, (PartId, Map[V, Set[Unit]], Map[V, Set[Unit]])]
      val adj = nStreamToAdj(neighbours, bldr, p)

      println(s"Finish parsing Neighbourhood stream to adj Builder @ $timeNow")

      LogicalParGraph.fromAdjList[V, HPair](adj.toMap)
    }

    def loomPartitioning(qStream: QStream[V, HPair], edges: Iterator[HPair[V]]) = {

      println(s"Create the partitioner @ $timeNow")

      //TODO: Introduce yet more parameters for the length of the stream to "train" the trie with and the motif threshold
      val trie = qStream.take(40).map(_._2).foldLeft(TPSTry.empty[V, HPair, P](conf.prime)) { (trie, g) =>
        trie.add(g)
      }
      val motifs = trie.motifsFor(0.5)

      //Create the Loom partitioner for LogicalParGraph, V, HPair
      //TODO: Introduce parameters for window size and alpha
      val p = Loom[LogicalParGraph, V, HPair, P](conf.numV/conf.numK, Map.empty[PartId, Int], conf.numK,
        motifs, 1000, 1.5, conf.prime)


      val bldr = mutable.Map.empty[V, (PartId, Map[V, Set[Unit]], Map[V, Set[Unit]])]
      val adj = eStreamToAdj(edges, bldr, p)
      LogicalParGraph.fromAdjList[V, HPair](adj.toMap)
    }

    //For each stream order
    List(conf.bfs, conf.dfs, conf.rand).map { order =>
      //Get json dump = - create Stream[UNeighbourhood[V, HPair]] with GraphReader
      println(s"[INFO]------ Running experiment with graph at path: $order")

      //Fold in order to deal with possible parsing errors
      GraphReader.readNeighbourhoods(order).fold({ e =>
        val error = s"error parsing json file $order on line ${e.line}: ${e.msg}"
        System.err.println(error)
        error
      }, { neighbours =>

        println(s"Finished parsing json @ $timeNow")
        val qStream = exp.fixedQueryStream(Map("q1" -> 0.5, "q2" -> 0.3, "q3" -> 0.2))

//        val g = altPartitioning(neighbours)
        val g = loomPartitioning(qStream, neighbours.flatMap(n => n.edges))

        val pSummaries = g.partitions.foreach { p =>

          val eSample = p.edges.map(e => ("""[A-Za-z]+\(""".r findAllIn e.toString).mkString(" -> "))
          val counts = mutable.HashMap[String, Int]()
          while(eSample.hasNext) {
            val eString = eSample.next()
            val eCount = counts.getOrElse(eString, 0)
            counts(eString) = eCount + 1
          }
          println("============")
          println(s"${counts.toString()}")
          println("============")
        }

        val pSizes = g.partitions.map(_.size).mkString("(", ", ", ")")

        println(s"Finished construction graph @ $timeNow")

        println(s"Partition sizes are: $pSizes")


        println(s"Create experiment @ $timeNow")


        println(s"Start running experiment @ $timeNow")

        //Test queries
        exp.trial(g)

        //Run the experiment
        val results = exp.run(40, qStream, g)

        println(s"Finish running experiment @ $timeNow")

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
