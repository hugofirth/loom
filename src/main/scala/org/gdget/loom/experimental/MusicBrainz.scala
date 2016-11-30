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
import org.gdget.{Direction, HPair}
import org.gdget.data.SimpleGraph
import org.gdget.data.query.QueryBuilder
import org.gdget.loom.{Field, Labelled}
import org.gdget.loom.experimental.Experiment.Q
import org.gdget.partitioned.data.LogicalParGraph
import org.gdget.partitioned.{PartId, Partitioned}

import scala.util.Random


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

    implicit val mBVParsable = new Parsable[Vertex] {
      /** Factory method for Vertex given a label string and an id Int */
      override def fromRepr(lbl: String, id: Int): Either[String, Vertex] = (lbl, id) match {
        case ("Album", i) =>
          Right(Album(i, None))
        case ("Area", i) =>
          Right(Area(i, None))
        case ("Artist", i) =>
          Right(Artist(i, None))
        case ("ArtistAlias", i) =>
          Right(ArtistAlias(i, None))
        case ("ArtistCredit", i) =>
          Right(ArtistCredit(i, None))
        case ("Country", i) =>
          Right(Country(i, None))
        case ("Entry", i) =>
          Right(Entry(i, None))
        case ("Label", i) =>
          Right(Label(i, None))
        case ("Medium", i) =>
          Right(Medium(i, None))
        case ("Other", i) =>
          Right(Other(i, None))
        case ("Place", i) =>
          Right(Place(i, None))
        case ("Recording", i) =>
          Right(Recording(i, None))
        case ("Release", i) =>
          Right(Release(i, None))
        case ("SearchHint", i) =>
          Right(SearchHint(i, None))
        case ("Single", i) =>
          Right(Single(i, None))
        case ("Track", i) =>
          Right(Track(i, None))
        case ("Url", i) =>
          Right(Url(i, None))
        case ("Work", i) =>
          Right(Work(i, None))
        case ("", i) =>
          Right(Other(i, None))
        case other =>
          Left(s"Unrecognised vertex label $other")
      }

      /** Factory method for Vertex given an Edge label string, an id Int and a direction */
      override def fromEdgeRepr(eLbl: String, id: Int, dir: Direction): Either[String, Vertex] = (eLbl, id, dir) match {
        case ("Album", i, _) =>
          Right(Album(i, None))
        case ("Area", i, _) =>
          Right(Area(i, None))
        case ("Artist", i, _) =>
          Right(Artist(i, None))
        case ("ArtistAlias", i, _) =>
          Right(ArtistAlias(i, None))
        case ("ArtistCredit", i, _) =>
          Right(ArtistCredit(i, None))
        case ("Country", i, _) =>
          Right(Country(i, None))
        case ("Entry", i, _) =>
          Right(Entry(i, None))
        case ("Label", i, _) =>
          Right(Label(i, None))
        case ("Medium", i, _) =>
          Right(Medium(i, None))
        case ("Other", i, _) =>
          Right(Other(i, None))
        case ("Place", i, _) =>
          Right(Place(i, None))
        case ("Recording", i, _) =>
          Right(Recording(i, None))
        case ("Release", i, _) =>
          Right(Release(i, None))
        case ("SearchHint", i, _) =>
          Right(SearchHint(i, None))
        case ("Single", i, _) =>
          Right(Single(i, None))
        case ("Track", i, _) =>
          Right(Track(i, None))
        case ("Url", i, _) =>
          Right(Url(i, None))
        case ("Work", i, _) =>
          Right(Work(i, None))
        case ("", i, _) =>
          Right(Other(i, None))
        case other =>
          Left(s"Unrecognised vertex label $other")
      }
    }


    implicit val mBVPartitioned = new Partitioned[Vertex] {
      override def partition(v: Vertex): Option[PartId] = v.part
    }

    implicit val mBVShow = new Show[Vertex] {
      override def show(f: Vertex): String = f.toString
    }

    implicit val mBVEq = new Eq[Vertex] {
      override def eqv(x: Vertex, y: Vertex): Boolean = x.equals(y)
    }

    /** Generator for MusicBrainzVertex Labelled typeclass instance, needs to be local instantiated because needs prime p */
    def mBVLabelled[P <: Field](p: P) = new Labelled[Vertex] {

      val labels = Random.shuffle((0 until p.value).toVector).take(18)

      override def label(a: Vertex): Int = a match {
        case Album(_, _) => labels(0)
        case Area(_, _) => labels(1)
        case Artist(_, _) => labels(2)
        case ArtistAlias(_, _) => labels(3)
        case ArtistCredit(_, _) => labels(4)
        case Country(_, _) => labels(5)
        case Entry(_, _) => labels(6)
        case Label(_, _) => labels(7)
        case Medium(_, _) => labels(8)
        case Other(_, _) => labels(9)
        case Place(_, _) => labels(10)
        case Recording(_, _) => labels(11)
        case Release(_, _) => labels(12)
        case SearchHint(_, _) => labels(13)
        case Single(_, _) => labels(14)
        case Track(_, _) => labels(15)
        case Url(_, _) => labels(16)
        case Work(_, _) => labels(17)
      }
    }
  }

  /** Trait containing information needed for running MusicBrainz experiments, including graph location and queries */
  trait MusicBrainzExperimentMeta extends ExperimentMeta[Vertex, HPair] { self: Experiment[Vertex, HPair] =>

    //TODO: Most naive implementation possible from paper 1, need to improve query lang
    def q1 = {
      val op = QueryBuilder[LogicalParGraph, Vertex, HPair]
      for {
        as <- op.getAll[Area]
        ats <- as.filter(_ => Random.nextInt(100) > 89).traverse(op.traverseAllNeighbours[Artist])
        lbls <- ats.flatten.traverse(op.traverseAllNeighbours[Label])
        as2 <- lbls.flatten.traverse(op.traverseAllNeighbours[Area])
      } yield as2.flatten
    }

    def q2 = {
      val op = QueryBuilder[LogicalParGraph, Vertex, HPair]
      for {
        ats <- op.getAll[Artist]
        acs <- ats.filter(_ => Random.nextInt(100) > 89).traverse(op.traverseAllNeighbours[ArtistCredit])
        recs <- acs.flatten.traverse(op.traverseAllNeighbours[Recording])
        acs2 <- recs.flatten.traverse(op.traverseAllNeighbours[ArtistCredit])
        ats2 <- acs2.flatten.traverse(op.traverseAllNeighbours[Artist])
      } yield ats2.flatten
    }


    def q3 = {
      val op = QueryBuilder[LogicalParGraph, Vertex, HPair]
      for {
        ats <- op.getAll[Artist]
        acs <- ats.filter(_ => Random.nextInt(100) > 89).traverse(op.traverseAllNeighbours[ArtistCredit])
        tks <- acs.flatten.traverse(op.traverseAllNeighbours[Track])
        mds <- tks.flatten.traverse(op.traverseAllNeighbours[Medium])
      } yield mds.flatten
    }

//    val gQ1 = SimpleGraph[Vertex, HPair](
//      Entity(1, None) -> Entity(2, None),
//      Entity(2, None) -> Entity(3, None)
//    )
//
//    val gQ2 = SimpleGraph[Vertex, HPair](
//      Agent(1, None) ->  Activity(2, None),
//      Activity(2, None) -> Entity(3, None),
//      Entity(3, None) -> Entity(4, None),
//      Entity(4, None) -> Activity(5, None),
//      Activity(5, None) -> Agent(6, None)
//    )
//
//    val gQ3 = SimpleGraph[Vertex, HPair](
//      Agent(1, None) -> Activity(2, None)
//    )


    override def queries: Map[String, (Q[Vertex, HPair], SimpleGraph[Vertex, HPair])] =
      Map(
        "q1" -> (q1, gQ1),
        "q2" -> (q2, gQ2),
        "q3" -> (q3, gQ3)
      )
  }
}
