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

import org.gdget.partitioned._
import org.gdget.partitioned.data._
import org.gdget.data.query._
import org.gdget.loom.experimental.ProvGen.{Activity, Agent, Entity, Vertex => ProvGenVertex}
import org.gdget.{Edge, Graph}
import org.gdget.std.all._

import language.higherKinds
import scala.concurrent._

/** Description of Class
  *
  * @author hugofirth
  */
object Sandbox extends App {

  val (v1, v2, v3, v4, v5, v6) = (1 -> 1.part, 2 -> 1.part, 3 -> 1.part, 4 -> 2.part, 5 -> 2.part, 6 -> 2.part)

  type UTuple[A] = (A, A)

  val b: LogicalParGraph[(Int, PartId), UTuple] = LogicalParGraph[(Int, PartId), UTuple](
    v1 -> v4,
    v1 -> v5,
    v1 -> v6,
    v2 -> v1,
    v3 -> v2,
    v3 -> v1,
    v4 -> v3,
    v5 -> v2,
    v5 -> v6,
    v6 -> v3
  )

  val (p1,p2,p3,p4,p5) = (Entity(1, Option(1.part)),
                          Entity(2, Option(1.part)),
                          Activity(3, Option(2.part)),
                          Agent(4, Option(2.part)),
                          Activity(5, Option(2.part)))

  val c = LogicalParGraph[ProvGenVertex, UTuple](
    p1 -> p2,
    p1 -> p3,
    p3 -> p2,
    p3 -> p4,
    p2 -> p5
  )

  import cats.syntax.traverse._
  import cats._
  import cats.instances.all._
  import ExecutionContext.Implicits.global
  import LogicalParGraph._

  //Todo - make op object which means you only have to declare types once.
  //E.G. val op = QueryBuilder[G, V, E]
  // op.get(1)
  // op.traverseEdge(...)
  // If we think op.blah is cumbersome, then how about ...?

  //TODO: How to avoid having to flatten at the end?

  //TODO: What about a Queryable function which takes a Graph and a ParScheme. Perhaps also an implicit QueryBuilder
  //  which I could then use to prop up type inference?

  def query = {
    val op = QueryBuilder[LogicalParGraph, (Int, PartId), UTuple]
    for {
      v <- op.get(v1)
      p <- v.traverse(op.traverseEdge(_, (v1, v4)))
      _ <- op.traverseEdge(v4, (v4, v3))
    } yield p.flatten
  }

  def query2 = {
    val op = QueryBuilder[LogicalParGraph, ProvGenVertex, UTuple]
    for {
      as <- op.getWhere {
        case Activity(_, _) => true
        case _ => false
      }
      es <- as.traverse(op.traverseNeighboursWhere(_) {
        case Entity(_, _) => true
        case _ => false
      })
      e <- op.where(es)(_.size > 1)
    } yield e
  }




  val otherResult = query2.transK[Id].run(c)

  println(otherResult)

  import org.gdget.loom.experimental._

  val interpreter = countingInterpreterK[Future, (Int, PartId), UTuple]
  val result = query.transKWith[Future](interpreter).run(b)
  result.onSuccess {
    case Some(edge) => println(s"Result is: $edge and took ${interpreter.iptCount} traversals to evaluate.")
    case None => println(s"The query returns nothing and took ${interpreter.iptCount} traversals to evaluate.")
  }


}
