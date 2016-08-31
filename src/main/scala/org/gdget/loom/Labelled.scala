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

import cats.data.NonEmptyList

import scala.annotation.implicitNotFound

/** Simple typeclass which requires implementing types to associate an Int "label" with instances.
  *
  * The intended use case of this typeclass is to associate distinct Int labels with the subtypes of a Vertex type
  * in a graph. E.g:
  *
  * {{{
  * sealed trait MyVertex
  * case class A(...) extends MyVertex // => 1
  * case class B(...) extends MyVertex // => 7
  * case class C(...) extends MyVertex // => 2
  *
  * object MyVertex {
  *   implicit val myVertexlabel = new Label[MyVertex] {
  *
  *     def label(v: MyVertex) = v match {
  *       case A(...) => 1
  *       case B(...) => 7
  *       case C(...) => 2
  *     }
  *   }
  * }
  * }}}
  */
@implicitNotFound("No member of type class Labelled found for type ${A}")
trait Labelled[A] {

  def label(a: A): Int

}

object Labelled {

  @inline def apply[A: Labelled]: Labelled[A] = implicitly[Labelled[A]]

}

