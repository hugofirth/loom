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
import cats.implicits._
import org.gdget.{Direction, HPair, Out}
import org.gdget.data.SimpleGraph
import org.gdget.data.query.QueryBuilder
import org.gdget.loom.{Field, Labelled}
import org.gdget.loom.experimental.Experiment.Q
import org.gdget.partitioned._
import org.gdget.partitioned.data.LogicalParGraph

import scala.util.Random



object ProvGen {

  /** ADT for ProvGen vertices */
  sealed trait Vertex extends GenVertex {
    def canEqual(other: Any): Boolean = other.isInstanceOf[Vertex]
  }

  case class Agent(id: Int, part: Option[PartId]) extends Vertex

  case class Activity(id: Int, part: Option[PartId]) extends Vertex

  case class Entity(id: Int, part: Option[PartId]) extends Vertex

  object Vertex {

    /** ProvGenVertex typeclass instances */

    implicit val pGVParsable = new Parsable[Vertex] {
      /** Factory method for ProvGenVertex given a label string and an id Int */
      def fromRepr(lbl: String, id: Int): Either[String, Vertex] = (lbl, id) match {
        case ("AGENT", i) =>
          Right(Agent(i, None))
        case ("ACTIVITY", i) =>
          Right(Activity(i, None))
        case ("ENTITY", i) =>
          Right(Entity(i, None))
        case other =>
          Left(s"Unrecognised vertex label $other")
      }

      /** Factory method for ProvGenVertex given an Edge label string, an id Int and a direction */
      //TODO: Find out why on earth I'm calling .toInt here?
      def fromEdgeRepr(eLbl: String, id: Int, dir: Direction): Either[String, Vertex] = (eLbl, id, dir) match {
        case ("WASDERIVEDFROM", i, _) =>
          Right(Entity(i.toInt, None))
        case ("WASGENERATEDBY", i, d) =>
          Right(if(d == Out) Entity(i.toInt, None) else Activity(i.toInt, None))
        case ("WASASSOCIATEDWITH", i, d) =>
          Right(if(d == Out) Activity(i.toInt, None) else Agent(i.toInt, None))
        case ("USED", i, d) =>
          Right(if(d == Out) Activity(i.toInt, None) else Entity(i.toInt, None))
        case other =>
          Left(s"Unrecognised edge label $other")
      }
    }

    implicit val pGVPartitioned = new Partitioned[Vertex] {
      override def partition(v: Vertex): Option[PartId] = v.part
    }

    implicit val pGVShow = new Show[Vertex] {
      override def show(f: Vertex): String = f.toString
    }

    implicit val pGVEq = new Eq[Vertex] {
      override def eqv(x: Vertex, y: Vertex): Boolean = x.equals(y)
    }

    /** Generator for ProvGenVertex Labelled typeclass instance, needs to be local instantiated because needs prime p */
    def pgVLabelled[P <: Field](p: P) = new Labelled[Vertex] {

      val labels = Random.shuffle((0 until p.value).toVector).take(3)

      override def label(a: Vertex): Int = a match {
        case Entity(_, _) => labels(0)
        case Agent(_, _) => labels(1)
        case Activity(_, _) => labels(2)
      }
    }

  }

  /** Trait containing information needed for running ProvGen experiments, including graph location and queries */
  trait ProvGenExperimentMeta extends ExperimentMeta[Vertex, HPair] { self: Experiment[Vertex, HPair] =>

    //TODO: Most naive implementation possible from paper 1, need to improve query lang
    def q1 = {
      val op = QueryBuilder[LogicalParGraph, Vertex, HPair]
      for {
        es <- op.getAll[Entity]
        es2 <- es.filter(_ => Random.nextInt(100) > 89).traverse(op.traverseAllNeighbours[Entity])
        es3 <- es2.flatten.traverse(op.traverseAllNeighbours[Entity])
      } yield es3.flatten
    }

    def q2 = {
      val op = QueryBuilder[LogicalParGraph, Vertex, HPair]
      for {
        age <- op.getAll[Agent]
        act <- age.filter(_ => Random.nextInt(100) > 89).traverse(op.traverseAllNeighbours[Activity])
        ent <- act.flatten.traverse(op.traverseAllNeighbours[Entity])
        ent2 <- ent.flatten.traverse(op.traverseAllNeighbours[Entity])
        act2 <- ent2.flatten.traverse(op.traverseAllNeighbours[Activity])
        age2 <- act2.flatten.traverse(op.traverseAllNeighbours[Agent])
      } yield age2.flatten
    }


    def q3 = {
      val op = QueryBuilder[LogicalParGraph, Vertex, HPair]
      for {
        age <- op.getAll[Agent]
        act <- age.filter(_ => Random.nextInt(100) > 89).traverse(op.traverseAllNeighbours[Activity])
      } yield act.flatten
    }

    val gQ1 = SimpleGraph[Vertex, HPair](
      Entity(1, None) -> Entity(2, None),
      Entity(2, None) -> Entity(3, None)
    )

    val gQ2 = SimpleGraph[Vertex, HPair](
      Agent(1, None) ->  Activity(2, None),
      Activity(2, None) -> Entity(3, None),
      Entity(3, None) -> Entity(4, None),
      Entity(4, None) -> Activity(5, None),
      Activity(5, None) -> Agent(6, None)
    )

    val gQ3 = SimpleGraph[Vertex, HPair](
      Agent(1, None) -> Activity(2, None)
    )


    override def queries: Map[String, (Q[Vertex, HPair], SimpleGraph[Vertex, HPair])] =
      Map(
        "q1" -> (q1, gQ1),
        "q2" -> (q2, gQ2),
        "q3" -> (q3, gQ3)
      )
  }

}
