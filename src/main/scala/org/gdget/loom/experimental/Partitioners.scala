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

  //TODO: Move to proper error handling, by using either?
  require(capacity>0, s"You must indicate a partition capacity of greater than 0 to LDG. You have provided a capacity of $capacity")
  require(k>0, s"You must have 1 or more partitions! You have provided a k of $k")


  //TODO: Investigate possible resetting of pSizes map here
  val unused = (0 to k).map(_.part).filterNot(sizes.contains)

  val pSizes = sizes ++ unused.map(_ -> 0)

  def partitionOf[V: Partitioned, E[_]: Edge](n: UNeighbourhood[V, E],
                                                            adj: AbsMap[V, (PartId, _, _)]): Option[PartId] = {

    def minUsed[A](x: (A, Int), y: (A, Int)) = if (x._2 > y._2) y else x

    //Check if the neighbours of a vertex v are assigned yet (exist as entries in the adjacency matrix)
    //NOTE!!!!! The below only works if our vertices obey the partId != equality law
    val existingNeighbours = n.neighbours.flatMap(adj.get).toList
    val neighbourPartitions = existingNeighbours.map(_._1)
    val partitionCounts = neighbourPartitions.groupBy(identity).mapValues(_.size.toDouble)
    //Adjust partition counts to maintain balance and find the biggest scoring partition

    partitionCounts.reduceLeftOption { (highScore, current) =>
      //Get the score of the current partition
      val pScore = pSizes.get(current._1).fold(0D) { s => current._2 * (1 - (s / capacity)) }
      //Check if it is greater than the current highscore, then carry greatest scoring partition forward
      if(pScore > highScore._2) {
        current
      } else if(pScore == highScore._2) {
        val cSize = pSizes.get(current._1).map(current ->  _)
        val hSize = pSizes.get(highScore._1).map(highScore -> _)
        (cSize |@| hSize).map(minUsed).map(_._1).getOrElse(highScore)

      } else {
        highScore
      }
    }.orElse(pSizes.reduceLeftOption(minUsed)).map(_._1)
    //In the event that a vertex has no neighbours in any partition, assign them to the emptiest partition of the k
  }

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
          val pId = partitioner.partitionOf(input._2, input._1)
          val p = pId.map { id =>
            val size = partitioner.sizes.getOrElse(id, 0)
            partitioner.copy(sizes = partitioner.sizes.updated(id, size + 1))
          }
          (p.getOrElse(partitioner), pId)
      }
    }

}

sealed trait FennelPartitionerInstances
