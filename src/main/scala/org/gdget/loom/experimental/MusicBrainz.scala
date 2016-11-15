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

import cats._
import org.gdget.HPair
import org.gdget.data.SimpleGraph
import org.gdget.loom.experimental.Experiment.Q
import org.gdget.partitioned.{PartId, Partitioned}


object MusicBrainz {

  /** ADT for MusicBrainz vertices */
  sealed trait Vertex extends GenVertex {
    def canEqual(other: Any): Boolean = other.isInstanceOf[Vertex]
  }

  case class Album(id: Int, part: Option[PartId]) extends Vertex
  case class Area(id: Int, part: Option[PartId]) extends Vertex
  case class Artist(id: Int, part: Option[PartId]) extends Vertex
  case class ArtistAlias(id: Int, part: Option[PartId]) extends Vertex
  case class ArtistCredit(id: Int, part: Option[PartId]) extends Vertex
  case class Country(id: Int, part: Option[PartId]) extends Vertex
  case class Entry(id: Int, part: Option[PartId]) extends Vertex
  case class Label(id: Int, part: Option[PartId]) extends Vertex
  case class Medium(id: Int, part: Option[PartId]) extends Vertex
  case class Other(id: Int, part: Option[PartId]) extends Vertex
  case class Place(id: Int, part: Option[PartId]) extends Vertex
  case class Recording(id: Int, part: Option[PartId]) extends Vertex
  case class Release(id: Int, part: Option[PartId]) extends Vertex
  case class SearchHint(id: Int, part: Option[PartId]) extends Vertex
  case class Single(id: Int, part: Option[PartId]) extends Vertex
  case class Track(id: Int, part: Option[PartId]) extends Vertex
  case class Url(id: Int, part: Option[PartId]) extends Vertex
  case class Work(id: Int, part: Option[PartId]) extends Vertex

  object Vertex {

    /** MusicBrainz Vertex typeclass instances */

    implicit val mBVPartitioned = new Partitioned[Vertex] {
      override def partition(v: Vertex): Option[PartId] = v.part
    }

    implicit val mBVShow = new Show[Vertex] {
      override def show(f: Vertex): String = f.toString
    }

    implicit val mBVEq = new Eq[Vertex] {
      override def eqv(x: Vertex, y: Vertex): Boolean = x.equals(y)
    }
  }

  /** Trait containing information needed for running MusicBrainz experiments, including graph location and queries */
  trait MusicBrainzExperimentMeta extends ExperimentMeta[Vertex, HPair] { self: Experiment[Vertex, HPair] =>
    override def queries: Map[String, (Q[Vertex, HPair], SimpleGraph[Vertex, HPair])] = ???
  }
}
