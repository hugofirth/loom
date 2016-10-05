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

import cats.Foldable
import org.gdget.Edge
import org.gdget.data.UNeighbourhood
import org.gdget.partitioned.{ParGraph, PartId, Partitioned, Partitioner}

import scala.collection.{Map => AbsMap}
import language.higherKinds
import scala.collection.immutable.Queue

/** Description of Class
  *
  * @author hugofirth
  */
case class Loom[G[_, _[_]], V, E[_]](capacity: Int, sizes: Map[PartId, Int], k: Int, motifs: TPSTry[G, V, E],
                                     matchList: Map[V, (Set[E[V]], TPSTryNode[G, V, E])],
                                     window: Queue[E[V]])(implicit pEv: ParGraph[G, V, E], eEv: Edge[E])  {


  //TPSTry
  //Motif support threshold

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

        //TODO: Implement LOOM.
      }
    }
}
