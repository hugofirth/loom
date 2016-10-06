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

  def factorFor(e: E[V]): Int = {
    val (l, r) = Edge[E].vertices(e)
    //Calculatee's edge factor and degree factors for l & r
    val rDegFactor = Labelled[V].label(r) + 1
    val lDegFactor = Labelled[V].label(l) + 1
    val eFactor = Labelled[V].label(l) - Labelled[V].label(r)
    //Calculate combined factor and new signature
    rDegFactor * lDegFactor * eFactor
  }

  def newMatchesGiven(e: E[V]): Map[V, Set[(Set[E[V]], TPSTryNode[G, V, E])]] = {
    //Get motif matches for both vertices in the edge
    val (l, r) = Edge[E].vertices(e)
    val motifMatches = (matchList.get(l) |+| matchList.get(r)).getOrElse(Set.empty[(Set[E[V]], TPSTryNode[G, V, E])])
    //For each motif match
    val matches = for {
      //Get the associated TPSTry node
      (edges, node) <- motifMatches
      //If node has a child with factor of e then new motif!
      c <- node.children.get(factorFor(e))
    } yield (edges + e, c)
    Map(l -> matches, r -> matches)
    //TODO: Part 2 of algo around merging
  }


  //TPSTry
  //Motif support threshold

  def addToWindow(e: E[V]): List[(E[V], PartId)] = {

    //Check if e is a motif, if not then assign immediately
    motifs.root.children.get(factorFor(e))

    //If window is larger than t, then dequeueOption
    if(window.size >= t) {
      val assignee = window.dequeueOption

      //Sort motif matches and perform equal opportunism
    } else {
      // Add e, and work out new motif matches etc...


      //TODO: Work through the relationship between Neighbourhoods, Edges and Vertices here.
    }

    ???
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

  //Does it make sense to make the


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
