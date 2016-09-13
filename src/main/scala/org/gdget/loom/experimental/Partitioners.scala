package org.gdget.loom.experimental
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

import org.gdget.Edge
import org.gdget.data.UNeighbourhood

import language.higherKinds
import org.gdget.partitioned._


/** The LDG streaming graph partitioner described by Stanton & Kliot (http://dl.acm.org/citation.cfm?id=2339722) */
case class LDGPartitioner[G[_, _[_]], V, E[_]](capacity: Int, pSizes: Map[PartId, Int], k: Int)
                                              (implicit gEv: ParGraph[G, V, E], vEv: Partitioned[V], eEv: Edge[E]) {

  //Note: Not carrying state properly. Need to pass graph to constructor and add to it as we partition vertices.
  //This is a slightly bizarre design choice looking back on it, as if we immediately read the resulting stream of

  def partitionOf(n: UNeighbourhood[V, E], g: G[V, E]): Option[PartId] = {

    //Check if the neighbours of a vertex v are assigned yet (are in g)
    //NOTE!!!!! The below only works if our vertices obey the partId != equality law
    val existingNeighbours = ParGraph[G, V, E].findVertices(g)(n.neighbours.contains).toList
    val neighbourPartitions = for {
      n <- existingNeighbours
      part <- Partitioned[V].partition(n)
    } yield (n, part)
    val partitionCounts = neighbourPartitions.groupBy(_._2).mapValues(_.size.toDouble)
    //Adjust partition counts to maintain balance and find the biggest scoring partition
    partitionCounts.reduceLeftOption { (highScore, current) =>
      //Get the score of the current partition
      val pScore = pSizes.get(current._1).fold(0D) { s => current._2 * (1 - (s / capacity)) }
      //Check if it is greater than the current highscore, then carry greatest scoring partition forward
      if(pScore > highScore._2) current else highScore
    }.map(_._1)
  }

  //Above, how are we breaking ties? We should be assigning ties to the emptier of two parts. Walk through to make sure
  //we're really filling out k partitioners.

}



/** The Fennel partitioner described by Tsourakakis et al. (http://dl.acm.org/citation.cfm?id=2556213) */
case class FennelPartitioner(a: Int) {

}



object Partitioners extends LDGPartitionerInstances with FennelPartitionerInstances {

}

sealed trait LDGPartitionerInstances {

  implicit def lDGPartitioner[G[_, _[_]], V, E[_]](implicit gEv: ParGraph[G, V, E], vEv: Partitioned[V], eEv: Edge[E]) =
    new Partitioner[LDGPartitioner[G, ?, E]] {

      override def partition(partitioner: LDGPartitioner[G, V, E],
                             input: (G[V, E], UNeighbourhood[V, E])): (LDGPartitioner[G, V, E], Option[PartId]) = {

        (partitioner, partitioner.partitionOf(input._2, input._1))
      }
    }

}

sealed trait FennelPartitionerInstances
