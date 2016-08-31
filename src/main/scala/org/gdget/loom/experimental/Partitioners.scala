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

import language.higherKinds
import org.gdget.partitioned._

/** The LDG streaming graph partitioner described by Stanton & Kliot (http://dl.acm.org/citation.cfm?id=2339722) */
case class LDGPartitioner[V: Partitioned](capacity: Int, pSizes: Map[PartId, Int], k: Int, unassigned: Map[V, List[V]]) {


  def partitionOf[G[_, _[_]], E[_]](vertex: V, g: G[V, E])(implicit gEv: ParGraph[G, V, E], eEv: Edge[E]): PartId = {
    //Get the neighbourhood of vertex
    //Check if the neighbours are assigned yet (are in g) and not just to the temp partition
    //TODO: work out exactly how the temp partition (-1) is going to work.
    val neighbours = ParGraph[G, V, E].neighbourhood(g, vertex).fold(Set.empty[V])(_.neighbours)
    val neighbourPartitions = for {
      n <- neighbours
      part <- ParGraph[G, V, E].partitionOf(g, n) if part != (-1).part
    } yield (n, part)
    val partitionCounts = neighbourPartitions.groupBy(_._2).mapValues(_.size.toDouble)
    //Function to adjust partition counts to maintain balance and find the biggest scoring partition
    partitionCounts.reduceLeft{ (highScore, current) =>
      //Get the score of the current partition
      val pScore = pSizes.get(current._1).fold(0D) { s => current._2 * (1 - (s / capacity)) }
      //Check if it is greater than the current highscore, then carry greatest scoring partition forward
      if(pScore > highScore._2) current else highScore
    }._1
  }

}



/** The Fennel partitioner described by Tsourakakis et al. (http://dl.acm.org/citation.cfm?id=2556213) */
case class FennelPartitioner(a: Int) {

}

object Partitioners extends LDGPartitionerInstances with FennelPartitionerInstances {

}

sealed trait LDGPartitionerInstances {


}

sealed trait FennelPartitionerInstances
