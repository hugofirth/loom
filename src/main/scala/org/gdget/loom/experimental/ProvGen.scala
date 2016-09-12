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
package org.gdget.loom.experimental

import cats.{Eq, Show}
import org.gdget.HPair
import org.gdget.loom.experimental.Experiment.Q
import org.gdget.partitioned._



object ProvGen {

  /** ADT for ProvGen vertices
    *
    * @author hugofirth
    */
  sealed trait Vertex {
    def id: Int

    def part: Option[PartId]

    def canEqual(other: Any): Boolean = other.isInstanceOf[Vertex]

    override def equals(other: Any): Boolean = other match {
      case that: Vertex =>
        (that canEqual this) &&
          id == that.id
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(id)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

  }

  case class Agent(id: Int, part: Option[PartId]) extends Vertex

  case class Activity(id: Int, part: Option[PartId]) extends Vertex

  case class Entity(id: Int, part: Option[PartId]) extends Vertex

  object Vertex {

    /** ProvGenVertex typeclass instances */

    implicit val pGVPartitioned = new Partitioned[Vertex] {
      override def partition(v: Vertex): Option[PartId] = v.part
    }

    implicit val pGVShow = new Show[Vertex] {
      override def show(f: Vertex): String = f.toString
    }

    implicit val pGVEq = new Eq[Vertex] {
      override def eqv(x: Vertex, y: Vertex): Boolean = x.equals(y)
    }
  }

  /** Trait containing information needed for running ProvGen experiments, including graph location and queries */
  trait ProvGenExperimentMeta extends ExperimentMeta[Vertex, HPair] { self: Experiment[Vertex, HPair] =>

    override def queries: Map[String, Q[Vertex, HPair]] = ???
  }

}
