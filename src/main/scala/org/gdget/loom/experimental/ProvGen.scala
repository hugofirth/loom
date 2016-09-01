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
import org.gdget.partitioned._



object ProvGen {



  /** ADT for ProvGen vertices
    *
    * @author hugofirth
    */
  sealed trait ProvGenVertex {
    def id: Int

    def part: Option[PartId]

    def canEqual(other: Any): Boolean = other.isInstanceOf[ProvGenVertex]

    override def equals(other: Any): Boolean = other match {
      case that: ProvGenVertex =>
        (that canEqual this) &&
          id == that.id
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(id)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

  }

  case class Agent(id: Int, part: Option[PartId]) extends ProvGenVertex

  case class Activity(id: Int, part: Option[PartId]) extends ProvGenVertex

  case class Entity(id: Int, part: Option[PartId]) extends ProvGenVertex

  object ProvGenVertex {

    /** ProvGenVertex typeclass instances */

    implicit val pGVPartitioned = new Partitioned[ProvGenVertex] {
      override def partition(v: ProvGenVertex): Option[PartId] = v.part
    }

    implicit val pGVShow = new Show[ProvGenVertex] {
      override def show(f: ProvGenVertex): String = f.toString
    }

    implicit val pGVEq = new Eq[ProvGenVertex] {
      override def eqv(x: ProvGenVertex, y: ProvGenVertex): Boolean = x.equals(y)
    }
  }

}
