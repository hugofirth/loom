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

  //TODO: Move to proper error handling, by using either?, probably with private constructor and an Option[] factory?
  require(capacity>0, s"You must indicate a partition capacity of greater than 0 to LDG. You have provided a capacity of $capacity")
  require(k>0, s"You must have 1 or more partitions! You have provided a k of $k")

  private val unused = (0 until k).map(_.part).filterNot(sizes.contains)

  private val pSizes = sizes ++ unused.map(_ -> 0)

  def partitionOf[V: Partitioned, E[_]: Edge](n: UNeighbourhood[V, E],
                                                            adj: AbsMap[V, (PartId, _, _)]): Option[PartId] = {

    def minUsed[A](x: (A, Int), y: (A, Int)) = if (x._2 > y._2) y else x

    //Check if the neighbours of a vertex v are assigned yet (exist as entries in the adjacency matrix)
    //NOTE!!!!! The below only works if our vertices obey the partId != equality law
    val existingNeighbours = n.neighboursIterator.flatMap(adj.get)
    val neighbourPartitions = existingNeighbours.map(_._1).toList
    val partitionCounts = neighbourPartitions.groupBy(identity).mapValues(_.size.toDouble)
    //Adjust partition counts to maintain balance and find the biggest scoring partition

    //Backfill partitioncounts to include 0 entries for empty partitions
    val allPartitionCounts = (0 until k).map(_.part).map(p => p -> partitionCounts.getOrElse(p, 0D))
    //Calculate scores
    val scores = allPartitionCounts.map { case (pId, numNeighbours) =>
      //Find size of this partition, then weight neighbours by its size relative to max partition capacity "fullness"
      val pScore = pSizes.get(pId).fold(0D) { s => numNeighbours * (1 - s / capacity) }
      //Return pair of this partId and its score
      (pId, pScore)
    }
    scores.reduceLeftOption { (highScore, current) =>
      //Check if current's score is greater than the highscore, then carry greatest scoring partition forward
      if(current._2 > highScore._2) {
        current
      } else if(current._2 == highScore._2) {
        val cSize = pSizes.get(current._1).map(current -> _)
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
case class FennelPartitioner(sizes: Map[PartId, Int], k: Int, numV: Int, numE: Int) {


  private val unused = (0 until k).map(_.part).filterNot(sizes.contains)

  private val pSizes = sizes ++ unused.map(_ -> 0)

  def partitionOf[V: Partitioned, E[_]: Edge](n: UNeighbourhood[V, E], adj: AbsMap[V, (PartId, _, _)]): Option[PartId] = {

    def minUsed[A](x: (A, Int), y: (A, Int)) = if (x._2 > y._2) y else x

    /* Fennel seeks to assign v to Si which minimises:

       | N(v)/\ Pi | - alpha * theta/2 * (|Pi|)^(theta-1)

       where theta equals 1.5 and alpha equals sqrt(k) * (|E|)/(|V|^theta) */

    //Find the partition with the most of v's existing neighbours (i.e. greatest N(v) /\ Pi)
    val existingNeighbours = n.neighboursIterator.flatMap(adj.get)
    val neighbourPartitions = existingNeighbours.map(_._1).toList
    val partitionCounts = neighbourPartitions.groupBy(identity).mapValues(_.size.toDouble)

    //Find the partition which maximises the score for fennel's function

    //Backfill partitionCounts to include 0 entries for empty partitions
    val allPartitionCounts = (0 until k).map(_.part).map(p => p -> partitionCounts.getOrElse(p, 0D))

    //Filter out partitions which voilate 1.1 balance threshold
    val freePartitionCounts = allPartitionCounts.filter { case (pId, numNeighbours) =>
      pSizes.get(pId).exists(_ < (1.1 * numV/k))
    }

    //Calculate the scores
    //TODO: Rather than folding over the option, why not produce a list of Option then flattening?
    val scores = freePartitionCounts.map { case(pId, numNeighbours) =>
      //calculate alpha
      val alpha = math.sqrt(k) * (numE / math.pow(numV, 1.5))
      //calculate the score of the current partition
      val pScore = numNeighbours - (alpha * 0.75 * pSizes.get(pId).map(_.toDouble).fold(0D)(math.sqrt))
      //Return the pair of this partId and its score
      (pId, pScore)
    }

    scores.reduceLeftOption { (highScore, current) =>
      //If score is higher than current score, then carry greatest scoring partition forward
      if(current._2 > highScore._2) {
        current
      } else if(current._2 == highScore._2) {
        //If the scores are even break ties by selecting the partition with more free space
        val cSize = pSizes.get(current._1).map(current -> _)
        val hSize = pSizes.get(highScore._1).map(highScore -> _)
        (cSize |@| hSize).map(minUsed).map(_._1).getOrElse(highScore)
      } else {
        highScore
      }
    }.orElse(pSizes.reduceLeftOption(minUsed)).map(_._1)
    //In the event that a vertex has no neighbours in any partition, assign them to the emptiest partition of the k
  }
}

case class HashPartitioner(k: Int, nextPart: PartId)

object Partitioners {

  implicit def hashPartitioner[B, C] = new Partitioner[HashPartitioner, B, C, Option] {
    override def partition[CC <: C](partitioner: HashPartitioner, input: B,
                                    context: CC): (HashPartitioner, Option[(B, PartId)]) = {
      val nextId = (partitioner.nextPart.id + 1)%partitioner.k
      (partitioner.copy(nextPart = nextId.part), Option(input, partitioner.nextPart))
    }

    override implicit def F = Foldable[Option]

  }

  implicit def lDGPartitioner[V: Partitioned, E[_]: Edge] =
    new Partitioner[LDGPartitioner, UNeighbourhood[V , E], AbsMap[V, (PartId, _, _)], Option] {

      override implicit def F = Foldable[Option]

      override def partition[CC <: AbsMap[V, (PartId, _, _)]](partitioner: LDGPartitioner, input: UNeighbourhood[V , E],
                                                         context: CC): (LDGPartitioner, Option[(UNeighbourhood[V , E], PartId)]) = {

          val pId = partitioner.partitionOf(input, context)
          val p = pId.map { id =>
            val size = partitioner.sizes.getOrElse(id, 0)
            partitioner.copy(sizes = partitioner.sizes.updated(id, size + 1))
          }
          (p.getOrElse(partitioner), pId.map((input, _)))
      }
    }

  implicit def fennelPartitioner[V: Partitioned, E[_]: Edge] =
    new Partitioner[FennelPartitioner, UNeighbourhood[V, E], AbsMap[V, (PartId, _, _)], Option] {

      override implicit def F = Foldable[Option]

      override def partition[CC <: AbsMap[V, (PartId, _, _)]](partitioner: FennelPartitioner,
                                                              input: UNeighbourhood[V, E],
                                                              context: CC): (FennelPartitioner, Option[(UNeighbourhood[V, E], PartId)]) = {

        val pId = partitioner.partitionOf(input, context)
        val p = pId.map { id =>
          val size = partitioner.sizes.getOrElse(id, 0)
          partitioner.copy(sizes = partitioner.sizes.updated(id, size + 1))
        }
        (p.getOrElse(partitioner), pId.map((input, _)))
      }
    }

}




