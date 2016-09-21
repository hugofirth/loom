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

import cats._
import cats.implicits._
import org.gdget.Edge
import org.gdget.data.UNeighbourhood

import language.higherKinds
import org.gdget.partitioned._

import scala.collection.{Map => AbsMap}


/** The LDG streaming graph partitioner described by Stanton & Kliot (http://dl.acm.org/citation.cfm?id=2339722) */
case class LDGPartitioner(capacity: Int, sizes: Map[PartId, Int], k: Int) {

  val unused = (0 to k).map(_.part).filterNot(sizes.contains)

  val pSizes = sizes ++ unused.map(_ -> 0)

  def partitionOf[V: Partitioned, E[_]: Edge](n: UNeighbourhood[V, E],
                                                            adj: AbsMap[V, (PartId, _, _)]): Option[PartId] = {
    //Check if the neighbours of a vertex v are assigned yet (exist as entries in the adjacency matrix)
    //NOTE!!!!! The below only works if our vertices obey the partId != equality law
    val existingNeighbours = adj.filterKeys(n.neighbours.contains)
    val neighbourPartitions = for {
      (n, _) <- existingNeighbours
      part <- Partitioned[V].partition(n)
    } yield (n, part)
    val partitionCounts = neighbourPartitions.groupBy(_._2).mapValues(_.size.toDouble)
    //Adjust partition counts to maintain balance and find the biggest scoring partition
    partitionCounts.reduceLeftOption { (highScore, current) =>
      //Get the score of the current partition
      val pScore = pSizes.get(current._1).fold(0D) { s => current._2 * (1 - (s / capacity)) }
      //Check if it is greater than the current highscore, then carry greatest scoring partition forward
      if(pScore > highScore._2) {
        current
      } else if(pScore == highScore._2) {
        val minUsed = (x: ((PartId, Double), Int), y: ((PartId, Double), Int)) => if (x._2 > y._2) y._1 else x._1
        (pSizes.get(current._1).map((current, _)) |@|
          pSizes.get(highScore._1).map((highScore, _))).map(minUsed).getOrElse(highScore)
      } else {
        highScore
      }
    }.map(_._1)
    //TODO: Do something in the event of None which returns the equal smallest partitionId from pSizes. This will make
    // sure we're really filling out k partitioners.
  }

  //TODO: Above, walk through to

}



/** The Fennel partitioner described by Tsourakakis et al. (http://dl.acm.org/citation.cfm?id=2556213) */
case class FennelPartitioner(a: Int) {

}



object Partitioners extends LDGPartitionerInstances with FennelPartitionerInstances {

}

sealed trait LDGPartitionerInstances {

  implicit def lDGPartitioner[V: Partitioned, E[_]: Edge] =
    new Partitioner[LDGPartitioner, (AbsMap[V, (PartId, _, _)], UNeighbourhood[V , E])] {

      override def partition[BB <: (AbsMap[V, (PartId, _, _)], UNeighbourhood[V, E])]
        (partitioner: LDGPartitioner, input: BB): (LDGPartitioner, Option[PartId]) = {
          (partitioner, partitioner.partitionOf(input._2, input._1))
      }
    }

}

sealed trait FennelPartitionerInstances
