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

import scala.collection.{mutable, Map => AbsMap, Set => AbsSet}
import language.higherKinds
import scala.annotation.tailrec
import scala.collection.immutable.{Queue, SortedSet}

/** The Loom graph partitioner. TODO: Expand this comment ....
  *
  * TOOD: Investigate making the window mutable
  *
  * @author hugofirth
  */
final class Loom[G[_, _[_]], V: Partitioned : Labelled, E[_]: Edge] private (capacity: Int, pSizes: Map[PartId, Int], k: Int,
                                                                             motifs: TPSTry[V, E],
                                                                             matchList: Map[V, Set[(Set[E[V]], TPSTryNode[V, E])]],
                                                                             window: mutable.LinkedHashSet[E[V]],
                                                                             t: Int, alpha: Int, prime: Int)
                                                                            (implicit pEv: ParGraph[G, V, E])  {

  import Loom._

  private def minUsed[A](x: (A, Int), y: (A, Int)) = if (x._2 > y._2) y else x

  private def factorFor(e: E[V], context: Set[E[V]]): Int = {
    val (l, r) = Edge[E].vertices(e)
    //Work out existing degree for l & r
    //Create simplegraph from parent repr
    val pG = SimpleGraph((context + e).toSeq:_*)
    //Get neighbourhoods for both l & r in pG and find out their current degree, or else its 0
    val lDeg = Graph[SimpleGraph, V, E].neighbourhood(pG, l).map(_.neighbours.size).getOrElse(0)
    val rDeg = Graph[SimpleGraph, V, E].neighbourhood(pG, r).map(_.neighbours.size).getOrElse(0)

    //Calculate's edge factor and degree factors for l & r
    val rDegFactor = ((Labelled[V].label(r) + (rDeg + 1)).abs % prime) + 1
    val lDegFactor = ((Labelled[V].label(l) + (lDeg + 1)).abs % prime) + 1
    val eFactor = ((Labelled[V].label(l) - Labelled[V].label(r)).abs % prime) + 1
    //Calculate combined factor and new signature
    rDegFactor * lDegFactor * eFactor
  }

  private def newMatchesGiven(e: E[V]): Map[V, Set[(Set[E[V]], TPSTryNode[V, E])]] = {
    @tailrec
    def mergeMotifs(es: Set[E[V]], node: TPSTryNode[V, E], matchEs: Set[E[V]]): Option[(Set[E[V]], TPSTryNode[V, E])] = {
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
      (rEdges,  rNode) <- matchList.getOrElse(r, Set.empty[(Set[E[V]], TPSTryNode[V, E])])
      mergeMatch <- mergeMotifs(rEdges, lNode, lEdges)
    } yield mergeMatch

    val allMatches = lMatches |+| rMatches |+| mergedMatches
    Map(l -> allMatches, r -> allMatches)
  }

  def intersectionWithPart(g: Set[E[V]], part: PartId, context: AbsMap[V, (PartId, _, _)]) = {
    //Get the vertices from a set of edges
    val vertices = g.map(Edge[E].vertices).flatMap(edge => Set(edge._1, edge._2))
    //Get the list of PartIds for those vertices where they are currently assigned, then count how many are == part
    //toList call because we want more than 1 of the same PartId, so Set flatMap is dangerous
    vertices.toList.flatMap(context.get).count(_._1 == part)
  }

  def bid(part: PartId, m: (Set[E[V]], TPSTryNode[V, E]), context: AbsMap[V, (PartId, _, _)]) =
    intersectionWithPart(m._1, part, context) * (1 - (pSizes.getOrElse(part, 0).toDouble/capacity)) * m._2.support

  def ration(part: (PartId, Int), smin: (PartId, Int)) = {
    //Check that the partition whose bid is being rationed isn't smin, if it *is* then set alpha to 1
    val a = if(part == smin) 1 else alpha
    //Calculate and return ration l
    (part._2/smin._2) * a
  }

  private def equalOpportunism(e: E[V], context: AdjBuilder[V]): List[(E[V], PartId)] = {

    def intersectionWithPart(g: Set[E[V]], part: PartId, context: AbsMap[V, (PartId, _, _)]) = {
      //Get the vertices from a set of edges
      val vertices = g.map(Edge[E].vertices).flatMap(edge => Set(edge._1, edge._2))
      //Get the list of PartIds for those vertices where they are currently assigned, then count how many are == part
      //toList call because we want more than 1 of the same PartId, so Set flatMap is dangerous
      vertices.toList.flatMap(context.get).count(_._1 == part)
    }

    def bid(part: PartId, m: (Set[E[V]], TPSTryNode[V, E]), context: AbsMap[V, (PartId, _, _)]) =
      intersectionWithPart(m._1, part, context) * (1 - (pSizes.getOrElse(part, 0).toDouble/capacity)) * m._2.support

    def ration(part: (PartId, Int), smin: (PartId, Int)) = {
      //Check that the partition whose bid is being rationed isn't smin, if it *is* then set alpha to 1
      val a = if(part == smin) 1 else alpha
      //Calculate and return ration l
      (part._2/smin._2) * a
    }

    //get motif matches for edge to be assigned e
    val (l, r) = Edge[E].vertices(e)
    val matches = (matchList.get(l) |+| matchList.get(r)).getOrElse(Set.empty[(Set[E[V]], TPSTryNode[V, E])])
    //Sort in descending order of support
    val sortedMatches = matches.toList.sortBy({ case (_, n) => n.support })(Ordering[Int].reverse)
    //Find the least used partition. If there are no entries in pSizes then we go with a default of 1, to avoid
    //  divide by zero, though tbh  if pSizes is empty we have bigger problems. Need to be more systematic about my
    //  error handling.
    val smin = pSizes.reduceLeftOption(minUsed).getOrElse(0.part -> 1)
    //For each partition, calculate its ration
    val rations = pSizes.map(p => p._1 -> ration(p, smin))
    //For each partition calculate its total bid
    val bids = rations.map { case (pId, rtn) =>
      val biddable = sortedMatches.take(rtn)
      val total = biddable.foldLeft(0D){ (t, m) => t + bid(pId, m, context) }
      pId -> (total, biddable)
    }
    //Find the winner and drop its score
    val (winner, (_, winnerMotifs)) = bids.maxBy { case (pId, (score, bidMotifs)) => score }
    //Drop the tpstry nodes, combine the edge sets in the awarded (bid on) motif matches, and map them to the winning pId
    winnerMotifs.map(_._1).reduceOption(_ ++ _).fold(List.empty[E[V]])(_.toList).map(_ -> winner)
  }

  private def ldg(e: E[V], context: AdjBuilder[V]): (E[V], PartId) = {
    //TODO: The below relies heavily on assumptions about how the context AdjList is built, make sure they're correct
    def neighbourPartitions(v: V) = context.get(v).fold(List.empty[PartId]) { case (pId, in, out) =>
      pId :: (in.keySet ++ out.keySet).toList.flatMap(context.get).map(_._1)
    }

    //Get the vertices from e
    val (l,r) = Edge[E].vertices(e)
    //Get the adjLists of e's vertices from context if they exist
    val parts = neighbourPartitions(l) ++ neighbourPartitions(r)
    //TODO: add the negative weighting to make this actually like LDG
    //find the most common part or the least used partition
    val common = parts match {
      case ps @ hd :: tl => ps.groupBy(identity).mapValues(_.size).maxBy(_._2)._1
      case Nil => pSizes.reduceLeftOption(minUsed).map(_._1).getOrElse(0.part)
    }
    //return edge, common partId
    (e, common)
  }

  def addToWindow(e: E[V], context: AdjBuilder[V]): (Loom[G,V,E], List[(E[V], PartId)]) = {

    //Check if e is a motif, if not then assign immediately
    if(!motifs.root.children.contains(factorFor(e, Set.empty[E[V]]))) {
      //Assign E with LDG like heuristic
      (this, List(ldg(e, context)))
    } else {
      //If e *is* a motif, then add it to the window and update matchList
      val dWindow = window += e
      val dMatchList = matchList |+| newMatchesGiven(e)
      //Subsequently, if window is larger than t, dequeue oldest Edge and run equalOpportunism
      if(dWindow.size >= t) {
        dWindow.dequeueOption.fold((this.copy(matchList = dMatchList, window = dWindow), List.empty[(E[V], PartId)])) {
          case (assignee, ddWindow) =>
            val assignments = equalOpportunism(assignee, context)
            //Remove all edges to be assigned from the window
            //TODO: Use a mutable or at least heavily optimised queue variant for the window
            //TODO: Avoid frivolous filter and toSet calls if you can help it.
            val assignedEdges = assignments.map(_._1).toSet
            val dddWindow = ddWindow.filterNot(assignedEdges.contains)
            //Remove all motif matches which include assignedEdges from the matchList
            //First remove matchList entries which belong to vertices from assignedEdges
            //TODO: Pretty certain the below is not correct - check against algo in paper
            val assignedVertices = assignedEdges.map(Edge[E].vertices).flatMap(p => Set(p._1, p._2))
            //Then go through the entries for all vertices and update their sets to remove entries which include the edges
            val ddMatchList = (dMatchList -- assignedVertices).mapValues(_.filterNot { case (matchEs, node) =>
              matchEs.exists(assignedEdges.contains)
            })
            (this.copy(matchList = ddMatchList, window = dddWindow), assignments)
        }
      } else (this.copy(matchList = dMatchList, window = dWindow), List.empty[(E[V], PartId)])
    }
  }

}

object Loom {

  type AdjBuilder[V] = AbsMap[V, (PartId, AbsMap[V, Set[Unit]], AbsMap[V, Set[Unit]])]

  def apply[G[_, _[_]], V: Partitioned: Labelled, E[_]: Edge](capacity:Int, k: Int, motifs: TPSTry[V, E], t: Int,
                                                              alpha: Int, prime: Int)
                                                             (implicit pEv: ParGraph[G, V, E]): Loom[G, V, E] = {

    val emptyMatchList = Map.empty[V, Set[(Set[E[V]], TPSTryNode[V, E])]]
    val emptyWindow = mutable.LinkedHashSet.empty[E[V]]
    new Loom[G, V, E](capacity, (0 until k).map(_.part -> 0).toMap, k, motifs, emptyMatchList, emptyWindow, t, alpha, prime)

  }

  //TODO: In the rush to finish this we've lost some of the nice generalisation, try to add it back in later
  implicit def loomPartitioner[G[_, _[_]], V: Partitioned: Labelled, E[_]: Edge](implicit gEv: ParGraph[G, V, E]) =
    new Partitioner[Loom[G, V, E], E[V], AdjBuilder[V], List] {

      override implicit def F = Foldable[List]

      override def partition[CC <: AdjBuilder[V]](partitioner: Loom[G, V, E], input: E[V],
                                                  context: CC): (Loom[G, V, E], List[(E[V], PartId)]) = {

        //Partition the edge
        val (part, assignments) = partitioner.addToWindow(input, context)
        //Update sizes
        val sizeDeltas = assignments.groupBy(_._2).mapValues(_.size)
        val dSizes = part.sizes |+| sizeDeltas
        (part.copy(sizes = dSizes), assignments)
      }
    }
}
