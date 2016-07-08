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

import org.gdget.data.query._
import org.gdget.Edge

import org.gdget.data._
import org.gdget.std.all._
import language.higherKinds
import scala.concurrent._

/** Description of Class
  *
  * @author hugofirth
  */
object Sandbox extends App {

  val a = Map(
    1 -> (Set(2,3), Set(4,5,6)),
    2 -> (Set(5,3), Set(1)),
    3 -> (Set(6,4), Set(2,1)),
    4 -> (Set(1), Set(3)),
    5 -> (Set(1), Set(2,6)),
    6 -> (Set(1,5), Set(3))
  )

  type UTuple[A] = (A, A)

  val b: SimpleGraph[Int, UTuple] = SimpleGraph[Int, UTuple](
    1 -> 4,
    1 -> 5,
    1 -> 6,
    2 -> 1,
    3 -> 2,
    3 -> 1,
    4 -> 3,
    5 -> 2,
    5 -> 6,
    6 -> 3
  )

  implicitly[Edge[UTuple]]

  import SimpleGraph._
  import cats.syntax.traverse._
  import cats._
  import cats.std.all._
  import ExecutionContext.Implicits.global

  //TODO: Use kleisli composition to avoid having to flatten at the end?
  val query: QueryIO[SimpleGraph, Int, UTuple, Option[(Int, Int)]] = {
    for {
      v <- get[SimpleGraph, Int, UTuple](1)
      p <- v.traverse(traverseEdge[SimpleGraph, Int, UTuple](_, (1, 4)))
    } yield p.flatten
  }

  val result = query.transK[Future].run(b)
  result onSuccess {
    case Some(edge) => println(s"Result is: $edge")
  }

}
