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

import org.gdget.Direction

/** Simple typeclass to provide the parsing methods needed to produce vertex objects from the JSon input in
  * [[Main.jsonToNeighbourhood]]
  *
  * @see [[Main.jsonToNeighbourhood]]
  */
trait Parsable[V] {

  /** Factory method for ProvGenVertex given a label string and an id Int */
  def fromRepr(lbl: String, id: Int): Either[String, V]

  /** Factory method for ProvGenVertex given an Edge label string, an id Int and a direction */
  def fromEdgeRepr(eLbl: String, id: Int, dir: Direction): Either[String, V]
}

object Parsable {

  @inline final def apply[V: Parsable] = implicitly[Parsable[V]]
}