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
import org.gdget.Edge
import org.gdget.data.UNeighbourhood
import org.gdget.partitioned.{ParGraph, PartId, Partitioned, Partitioner}

import scala.collection.{Map => AbsMap}
import language.higherKinds
import scala.annotation.tailrec
import scala.collection.immutable.Queue

/** The Loom graph partitioner. TODO: Expand this comment ....
  *
  * @author hugofirth
  */
case class Loom[G[_, _[_]], V: Partitioned : Labelled, E[_]: Edge](capacity: Int, sizes: Map[PartId, Int], k: Int,
                                                                   motifs: TPSTry[G, V, E],
                                                                   matchList: Map[V, Set[(Set[E[V]], TPSTryNode[G, V, E])]],
                                                                   window: Queue[E[V]], t: Int)
                                                                  (implicit pEv: ParGraph[G, V, E])  {

  private implicit val matchOrd: Ordering[(Set[E[V]], TPSTryNode[G, V, E])] = Ordering.by { case(_, n) => n.support }

  private def factorFor(e: E[V], context: Set[E[V]]): Int = {
    val (l, r) = Edge[E].vertices(e)
    //Work out existing degree for l & r
    val vertexPairs = context.map(Edge[E].vertices)
    val lDeg = vertexPairs.count(pair => pair._1 == l || pair._2 == l)
    val rDeg = vertexPairs.count(pair => pair._1 == r || pair._2 == r)

    //Calculate's edge factor and degree factors for l & r
    val rDegFactor = Labelled[V].label(r) + (rDeg + 1)
    val lDegFactor = Labelled[V].label(l) + (lDeg + 1)
    val eFactor = Labelled[V].label(l) - Labelled[V].label(r)
    //Calculate combined factor and new signature
    rDegFactor * lDegFactor * eFactor
  }

  private def newMatchesGiven(e: E[V]): Map[V, Set[(Set[E[V]], TPSTryNode[G, V, E])]] = {
    @tailrec
    def mergeMotifs(es: Set[E[V]], node: TPSTryNode[G, V, E], matchEs: Set[E[V]]): Option[(Set[E[V]], TPSTryNode[G, V, E])] = {
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
      (rEdges,  rNode) <- matchList.getOrElse(r, Set.empty[(Set[E[V]], TPSTryNode[G, V, E])])
      mergeMatch <- mergeMotifs(rEdges, lNode, lEdges)
    } yield mergeMatch

    val allMatches = lMatches |+| rMatches |+| mergedMatches
    Map(l -> allMatches, r -> allMatches)
  }

  private def equalOpportunism(e: E[V], context: AbsMap[V, (PartId, _, _)]): List[(E[V], PartId)] = { ??? }

  private def lDG(e: E[V], context: AbsMap[V, (PartId, _, _)]): (E[V], PartId) = { ??? }

  def addToWindow(e: E[V], context: AbsMap[V, (PartId, _, _)]): (Loom[G,V,E], List[(E[V], PartId)]) = {

    //Check if e is a motif, if not then assign immediately
    if(!motifs.root.children.contains(factorFor(e, Set.empty[E[V]]))) {
      //Assign E with LDG like heuristic
      (this, List(lDG(e, context)))
    } else {
      //If e *is* a motif, then add it to the window and update matchList
      val dWindow = window.enqueue(e)
      val dMatchList = matchList |+| newMatchesGiven(e)
      //Subsequently, if window is larger than t, dequeue oldest Edge and run equalOpportunism
      if(dWindow.size >= t) {
        dWindow.dequeueOption.fold((this.copy(matchList = dMatchList, window = dWindow), List.empty[(E[V], PartId)])) {
          case (assignee, ddWindow) =>
            val assignments = equalOpportunism(assignee, context)
            //Remove all edges to be assigned from the window
            val assignedEdges = assignments.map(_._1).toSet
            val dddWindow = ddWindow.filterNot(assignedEdges.contains)
            //Remove all motif matches which include assignedEdges from the matchList
            //First remove matchList entries which belong to vertices from assignedEdges
            val assignedVertices = assignedEdges.map(Edge[E].vertices).flatMap(p => Set(p._1, p._2))
            //Then go through the entries for all vertices and update their sets to remove entries which include the edges
            val ddMatchList = (dMatchList -- assignedVertices).mapValues(_.filterNot { case (matchEs, node) =>
              matchEs.exists(assignedEdges.contains)
            })
            (this.copy(matchList = ddMatchList, window = dddWindow), assignments)
        }
      } else (this.copy(matchList = dMatchList, window = dWindow), List.empty[(E[V], PartId)])
    }
      //Then return Loom copy along with list of edges and partIds
      //TODO: Work through the relationship between Neighbourhoods, Edges and Vertices here.
  }


  //Either change to using a Partitioned[A] case class wrapper for partitioned element types or add a setPart method to
  //  existing partitioned typeclass

  //Refactor Partitioner typeclass to return an Option[V: Partitioned], Rather than an Option[PartId]?
  //How do we do buffered partitioners?
    //One option is abstract over Option in return type to some Foldable F[_]. That way some partitioners could return
    //  Option[V], another could return List[V].
    //Another option is to have another Partitioner typeclass for buffered partitioners which must return a V id as well
    //  as a partId, and the V id need not refer to the vertex just passed in. This pushes the window management up to
    //  the callsite. This seems like a bad idea.
  //We're going with abstracting to a Foldable TC F[_] and returning F[V] rather than an F[PartId].

  //Loom will still need the AdjListBuilder as an input like LDG
  //The window can be internal though.
}

object Loom {

  implicit def loomPartitioner[G[_, _[_]], V: Partitioned, E[_]: Edge](implicit gEv: ParGraph[G, V, E]) =
    new Partitioner[Loom[G, V, E], UNeighbourhood[V, E], AbsMap[V, (PartId, _, _)], List] {

      override implicit def F = Foldable[List]

      override def partition[CC <: AbsMap[V, (PartId, _, _)]](partitioner: Loom[G, V, E],
                                                              input: UNeighbourhood[V, E],
                                                              context: CC): (Loom[G, V, E], List[(UNeighbourhood[V, E], PartId)]) = {


        ???
        //TODO: Implement LOOM.
      }
    }
}
