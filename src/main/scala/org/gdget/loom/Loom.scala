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
package org.gdget.loom

import cats._
import cats.implicits._
import org.gdget.{Edge, Graph}
import org.gdget.data.{SimpleGraph, UNeighbourhood}
import org.gdget.partitioned._
import org.gdget.loom.util._

import scala.collection.{Map => AbsMap, Set => AbsSet}
import language.higherKinds
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.{Queue, SortedSet}
import scala.collection.mutable.ListBuffer

/** The Loom graph partitioner. TODO: Expand this comment ....
  *
  * TOOD: Investigate making the window mutable
  *
  * @author hugofirth
  */
final class Loom[G[_, _[_]], V: Partitioned : Labelled, E[_]: Edge, P <: Field] private (val capacity: Int,
                                                                                         val sizes: Map[PartId, Int],
                                                                                         val k: Int,
                                                                                         val motifs: TPSTry[V, E, P],
                                                                                         private val matchList: mutable.Map[V, Set[(Set[E[V]], TPSTryNode[V, E, P])]],
                                                                                         private val window: mutable.LinkedHashSet[E[V]],
                                                                                         private val windowSize: Int,
                                                                                         val t: Int,
                                                                                         val alpha: Double,
                                                                                         val prime: P)
                                                                                        (implicit pEv: ParGraph[G, V, E])  {

  import Loom._



  private def minUsed[A](x: (A, Int), y: (A, Int)) = if (x._2 > y._2) y else x

  private def factorFor(e: E[V], context: Set[E[V]]) = Signature.forAdditionToEdges(e, context, prime)

  private def newMatchesGiven(e: E[V]): Map[V, Set[(Set[E[V]], TPSTryNode[V, E, P])]] = {
    @tailrec
    def mergeMotifs(es: Set[E[V]], node: TPSTryNode[V, E, P], matchEs: Set[E[V]]): Option[(Set[E[V]], TPSTryNode[V, E, P])] = {
      //Only care about complete combinations of l & r motifs, sub combinations will be covered by other sub-motifs for r
      if(es.nonEmpty) {
        //Lazily calculate the combined factors for each edge in the r Motif, given the edges in the l Motif
        val eFactors = es.view.map(edge => (edge, factorFor(edge, matchEs)))
        //Find first edge in r Motif which has TPSTry node when added to l Motif
        //Could just yield the recursive call here, but tail recursion requires if or match, not flatMap/map
        val matchE = for {
          (edge, factor) <- eFactors.find(f => node.children.contains(f._2))
          c <- node.children.get(factor)
        } yield (es - edge, c, matchEs + edge)

        //Pattern match to enable tail rec, fold or whatever would be nicer :(
        matchE match {
          case Some((rM, c, lM)) => mergeMotifs(rM, c, lM)
          case None => None
        }
      } else Option((matchEs, node))
    }

    def getMotifMatches(v: V, e: E[V]) = {
      //Make sure to add root node with empty edge list to match list in order to check for base motif (w. 1 edge)
      val nilMotif = (Set.empty[E[V]], motifs.root)
      for {
        //Get motif matches for v including nilMotif
        (edges, node) <- matchList.get(v).fold(Set(nilMotif))(_ + nilMotif)
        //If node has a child with a factor of e then new motif!
        c <- node.children.get(factorFor(e, edges))
      } yield (edges + e, c)
    }

    //Get motif matches for both vertices in the edge
    val (l, r) = Edge[E].vertices(e)
    val lMatches = getMotifMatches(l, e)
    val rMatches = getMotifMatches(r, e)

    //Now on to the merging, take l matchList + lMatches and r matchList (not rMatches)
    val mergedMatches = for {
      (lEdges, lNode) <- matchList.get(l).fold(lMatches)(_ ++ lMatches)
      (rEdges,  rNode) <- matchList.getOrElse(r, Set.empty[(Set[E[V]], TPSTryNode[V, E, P])])
      mergeMatch <- mergeMotifs(rEdges, lNode, lEdges)
    } yield mergeMatch

    //TODO: Check performance for this semigroup add
    val allMatches = lMatches |+| rMatches |+| mergedMatches
    Map(l -> allMatches, r -> allMatches)
  }

  private def equalOpportunism(e: E[V], context: AdjBuilder[V]): Set[(E[V], PartId)] = {

    def intersectionWithPart(g: Set[E[V]], part: PartId, context: AbsMap[V, (PartId, _, _)]) = {
      //Get the vertices from a set of edges
      val vertices = g.map(Edge[E].vertices).flatMap(edge => Set(edge._1, edge._2))
      //Get the list of PartIds for those vertices where they are currently assigned, then count how many are == part
      //toList call because we want more than 1 of the same PartId, so Set flatMap is dangerous
      vertices.toList.flatMap(context.get).count(_._1 == part)
    }

    def bid(part: PartId, m: (Set[E[V]], TPSTryNode[V, E, P]), context: AbsMap[V, (PartId, _, _)]) =
      intersectionWithPart(m._1, part, context) * (1 - (sizes.getOrElse(part, 0).toDouble/capacity)) * m._2.support

    def ration(part: (PartId, Int), smin: (PartId, Int)) = {
      //Check that the partition whose bid is being rationed isn't smin, if it *is* then set alpha to 1
      val a = if(part == smin) 1.0 else alpha
      //Calculate and return ration l, rounded up to the nearest integer
      math.ceil((part._2/(smin._2 + 1)) * (1/a)).toInt
    }

    //get motif matches for edge to be assigned e
    val (l, r) = Edge[E].vertices(e)
    val matches = (matchList.get(l) |+| matchList.get(r)).getOrElse(Set.empty[(Set[E[V]], TPSTryNode[V, E, P])])
    //Sort in descending order of support
    val sortedMatches = matches.toList.sortBy({ case (_, n) => n.support })(Ordering[Int].reverse)
    //Find the least used partition. If there are no entries in sizes then we go with a default of 1, to avoid
    //  divide by zero, though tbh  if sizes is empty we have bigger problems. Need to be more systematic about my
    //  error handling.
    //TODO: Focus on this, as I'm not sure it quite works.
    val smin = sizes.reduceLeftOption(minUsed).getOrElse(0.part -> 1)
    //For each partition, calculate its ration
    val rations = sizes.map(p => p._1 -> ration(p, smin))
    //For each partition calculate its total bid
    val bids = rations.map { case (pId, rtn) =>
      val biddable = sortedMatches.take(rtn)
      val total = biddable.foldLeft(0D){ (t, m) => t + bid(pId, m, context) }
      pId -> (total, biddable)
    }
    //Find the winner and drop its score
    val (winner, (_, winnerMotifs)) = bids.maxBy { case (pId, (score, bidMotifs)) => score }
    //Drop the tpstry nodes, combine the edge sets in the awarded (bid on) motif matches, and map them to the winning pId
    val assignments = winnerMotifs.map(_._1).reduceOption(_ ++ _).getOrElse(Set.empty[E[V]]).map(_ -> winner)
    assignments + (e -> winner)
  }

  //TODO: Something terribly wrong with the performance of the below, must find out what
  private def ldg(e: E[V], context: AdjBuilder[V]): (E[V], PartId) = {

    //TODO: Find some way of passing in the whole neighbourhood here, its not fair otherwise.
    def neighbourPartitions(v: V) = context.get(v) match {
      case Some((pId, in, out)) => pBuffer(pId, in, out)
      case None => emptyBuffer
    }

    //Tracing to make this more VisualVM friendly
    def emptyBuffer = ListBuffer.empty[PartId]

    def pBuffer(pId: PartId, in: AbsMap[V, Set[Unit]], out: AbsMap[V, Set[Unit]]) = {
      ListBuffer(pId) ++= (in.keysIterator ++ out.keysIterator).flatMap(context.get).map(_._1)
    }

    //Get the vertices from e
    val (l,r) = Edge[E].vertices(e)
    //Get the adjLists of e's vertices from context if they exist
    val parts = neighbourPartitions(l) ++= neighbourPartitions(r)
    //find the most common part or the least used partition
    val partitionCounts = parts.groupBy(identity).mapValues(_.size.toDouble)
    //Adjust partition counts to maintain balance and find the biggest scoring partition

    //Backfill partitioncounts to include 0 entries for empty partitions
    val allPartitionCounts = (0 until k).map(_.part).map(p => p -> partitionCounts.getOrElse(p, 0D))

    //Calculate scores
    val scores = allPartitionCounts.map { case (pId, numNeighbours) =>
      //Find size of this partition, then weight neighbours by its size relative to max partition capacity "fullness"
      val pScore = sizes.get(pId).fold(0D) { s => numNeighbours * (1 - s / capacity) }
      //Return pair of this partId and its score
      (pId, pScore)
    }
    val common = scores.reduceLeftOption { (highScore, current) =>
      //Check if it is greater than the current highscore, then carry greatest scoring partition forward
      if(current._2 > highScore._2) {
        current
      } else if(current._2 == highScore._2) {
        val cSize = sizes.get(current._1).map(current -> _)
        val hSize = sizes.get(highScore._1).map(highScore -> _)
        (cSize |@| hSize).map(minUsed).map(_._1).getOrElse(highScore)

      } else {
        highScore
      }
    }.getOrElse(sizes.reduceLeft(minUsed))._1
    //In the event that a vertex has no neighbours in any partition, assign them to the emptiest partition of the k

    //return edge, common partId
    (e, common)
  }

  def addToWindow(e: E[V], context: AdjBuilder[V]): (Loom[G,V,E,P], Set[(E[V], PartId)]) = {

    //Check if e is a motif, if not then assign immediately
    if(!motifs.root.children.contains(factorFor(e, Set.empty[E[V]]))) {
      //Assign E with LDG like heuristic
      (this, Set(ldg(e, context)))
    } else {
      //If e *is* a motif, then add it to the window and update matchList
      val dWindowSize = this.windowSize + 1
      val dWindow = window += e
      val newMatches = newMatchesGiven(e)

      newMatches.foreach { m =>
        val existingEntries = matchList.getOrElse(m._1, Set.empty[(Set[E[V]], TPSTryNode[V, E, P])])
        matchList.update(m._1, existingEntries ++ m._2)
      }

      var newPart = new Loom(capacity, sizes, k, motifs, matchList, dWindow, dWindowSize, t, alpha, prime)

      //Subsequently, if window is larger than t, dequeue oldest Edge and run equalOpportunism
      if(dWindowSize >= t) {
        dWindow.headOption.fold((newPart, Set.empty[(E[V], PartId)])) { assignee =>
          val assignments = equalOpportunism(assignee, context)
          //Remove all edges to be assigned from the window
          val assignedEdges = assignments.map(_._1)

          val ddWindow = dWindow --= assignedEdges
          //Remove all motif matches which include assignedEdges from the matchList
          //First, find the set of vertices which are being assigned (as part of edges)
          val b = Set.newBuilder[V]
          for (e <- assignedEdges) {
            b += Edge[E].left(e)
            b += Edge[E].right(e)
          }
          val assignedVertices = b.result()

          //Then go through the entries for all these vertices and update their sets to remove motif matches which
          // include the assigned edges
          //Finally, go through the entries for other vertices in the removed motif matches and remove the matches
          // there as well
          //TODO: Improve performance. Below is better, but throws away all immutability, so we really ought to see big gains
          val dropB = new mutable.HashMap[V, mutable.Set[(Set[E[V]], TPSTryNode[V, E, P])]]
            with mutable.MultiMap[V, (Set[E[V]], TPSTryNode[V, E, P])]
          for {
            v <- assignedVertices
            entries <- matchList.get(v)
            (dropEdgeSet, dropNode)  <- entries if dropEdgeSet.exists(assignedEdges.contains)
            dropE <- dropEdgeSet
            (dropL, dropR) = Edge[E].vertices(dropE)
          } {
            dropB.addBinding(dropL, (dropEdgeSet, dropNode))
            dropB.addBinding(dropR, (dropEdgeSet, dropNode))
          }

          dropB.foreach { case (v, dropEntriesBuilder) =>
            val vEntries = matchList.getOrElse(v, Set.empty[(Set[E[V]], TPSTryNode[V, E, P])]) -- dropEntriesBuilder
            if(vEntries.isEmpty)
              matchList.remove(v)
            else
              matchList.update(v, vEntries)
          }

          //Create the new partitioner with the updated window and matchlist
          newPart = new Loom(capacity, sizes, k, motifs, matchList, ddWindow, dWindowSize - assignments.size, t, alpha, prime)
          (newPart, assignments)
        }
      } else (newPart, Set.empty[(E[V], PartId)])
    }
  }

}

object Loom {

  type AdjBuilder[V] = AbsMap[V, (PartId, AbsMap[V, Set[Unit]], AbsMap[V, Set[Unit]])]

  def apply[G[_, _[_]], V: Partitioned: Labelled, E[_]:Edge, P <: Field](capacity: Int,
                                                             sizes: Map[PartId, Int],
                                                             k: Int,
                                                             motifs: TPSTry[V, E, P],
                                                             t: Int,
                                                             alpha: Double,
                                                             prime: P)
                                                            (implicit pEv: ParGraph[G, V, E]): Loom[G, V, E, P] = {

    require(capacity>0, s"You must indicate a partition capacity of greater than 0 to Loom. You have provided a capacity of $capacity")
    require(k>0, s"You must have 1 or more partitions! You have provided a k of $k")

    val unused = (0 until k).map(_.part).filterNot(sizes.contains)

    val pSizes = sizes ++ unused.map(_ -> 0)

    val emptyMatchList = mutable.Map.empty[V, Set[(Set[E[V]], TPSTryNode[V, E, P])]]
    val emptyWindow = new mutable.LinkedHashSet[E[V]]()
    new Loom[G, V, E, P](capacity, pSizes, k, motifs, emptyMatchList, emptyWindow, 0, t, alpha, prime)
  }

  //TODO: In the rush to finish this we've lost some of the nice generalisation, try to add it back in later
  implicit def loomPartitioner[G[_, _[_]], V: Partitioned: Labelled, E[_]: Edge, P <: Field](implicit gEv: ParGraph[G, V, E]) =
    new Partitioner[Loom[G, V, E, P], E[V], AdjBuilder[V], List] {

      override implicit def F = Foldable[List]

      override def partition[CC <: AdjBuilder[V]](p: Loom[G, V, E, P], input: E[V],
                                                  context: CC): (Loom[G, V, E, P], List[(E[V], PartId)]) = {

       if(context.size % 10000 == 0 ) {
         //TODO: Work out why we get so many repeats (presumably drops) for bfs. Is the dropping a good strategy?
         println(s"Added ${context.size} vertices")
       }


        //Partition the edge

        //If the edge has been seen before, drop it
        if(!context.get(Edge[E].left(input)).fold(false)(_._3.contains(Edge[E].right(input)))) {

          val (part, assignments) = p.addToWindow(input, context)
          //Update sizes
          val sizeDeltas = assignments.groupBy(_._2).mapValues(_.size)
          val dSizes = part.sizes |+| sizeDeltas
          //p vs part fix
          (new Loom(part.capacity, dSizes, part.k, part.motifs, part.matchList, part.window, part.windowSize, part.t,
            part.alpha, part.prime), assignments.toList)
        } else {
          (p, List.empty[(E[V], PartId)])
        }
      }
    }
}
