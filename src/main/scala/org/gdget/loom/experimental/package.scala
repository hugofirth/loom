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

import language.higherKinds
import cats.data.Kleisli
import cats.{Monad, ~>}
import org.gdget.{Edge, Graph}
import org.gdget.data.query._
import org.gdget.partitioned.{LogicalParGraph, ParScheme}

/** Package object containing type definitions and helper functions used throughout the experiments of the LOOM system.
  *
  * @author hugofirth
  */
package object experimental {

  /** Interpreter which transforms a QuerOp object into a Kliesli function which takes a Graph (LogicalParGraph[S[_], V, E])
    * and produces an object of the desired result type, wrapped in provided Monad M[?].
    *
    * Additionally to assuming a specific Graph implementation, this query interpreter will count cross partition
    * traversals
    */
  def countingInterpreterK[M[_]: Monad, S[_]: ParScheme, V, E[_]: Edge] =
    new (QueryOp[LogicalParGraph[S, ?, ?[_]], V, E, ?] ~> Kleisli[M, LogicalParGraph[S, V, E], ?]) {
      import QueryOp._

      // Yucky mutable state, but what you gonna do? :( Right now the alternatives (StateT[M, ...] ?) are bending my brain.
      // TODO: Fix this interpreter to possibly use State. Or implement transST in gdget.data.query?
      // Note: This is especially bad as accessing this member requires reflection (why can't we extend ~>[_,_] again?)
      private var ipt = 0
      def iptCount = ipt 

      //TODO: Use ===
      def apply[A](fa: QueryOp[LogicalParGraph[S, ?, ?[_]], V, E, A]): Kleisli[M, LogicalParGraph[S, V, E], A] = {
        fa match {
          case TraverseEdge(v, e) =>
            fa.op { g =>
              if(g.partitionOf(v) != Edge[E].other(e, v).flatMap(g.partitionOf))
                ipt += 1
              val n = Graph[LogicalParGraph[S, ?, ?[_]]].neighbourhood(g, v)
              n.fold(None: Option[E[V]])(_.edges.find(_ == e))
            }
          case TraverseInNeighbour(v, in) =>
            fa.op { g =>
              if(g.partitionOf(v) != g.partitionOf(in))
                ipt += 1
              val n = Graph[LogicalParGraph[S, ?, ?[_]]].neighbourhood(g, v)
              n.fold(None: Option[V])(_.in.keySet.find(_ == in))
            }
          case TraverseOutNeighbour(v, out) =>
            fa.op { g =>
              if(g.partitionOf(v) != g.partitionOf(out))
                ipt += 1
              val n = Graph[LogicalParGraph[S, ?, ?[_]]].neighbourhood(g, v)
              n.fold(None: Option[V])(_.out.keySet.find(_ == out))
            }
          case _ => fa.defaultTransK[M]
        }
      }
    }


}
