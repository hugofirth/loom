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

import scala.collection.immutable.BitSet

/** Trait for objects which represent graph signatures
  *
  * @author hugofirth
  */
sealed trait Signature[P <: Field] {

  /** The set of distinct factors which make up this graph signature*/
  protected def factors: BitSet

  /** The factor multipliers, or number of times each factor appears in the signature, bit packed into an array of Ints
    *
    * Effectively a Map[Int, Int] where each Int -> Int entry is represented by a single Int (because each factor and its
    * multiplier are guaranteed to take less than 2 bytes to represent)
    */
  protected def factorM: Array[Int]

  /** The prime p which defines the finite field into which all factors must fall */
  def field: Field

  /** Get the product of all factors in this signature, as a BigInt */
  def get: BigInt

  /** Get the list of all factors in this signature */
  def getFactors: List[Int]

  override def toString: String = s"Signature(${this.get})"

  override def equals(other: Any): Boolean = other match {
    case that: Signature[_] =>
      factors == that.factors &&
        factorM.sameElements(that.factorM)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(factors, factorM)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

final class SignatureN[P <: Field] private (protected val factors: BitSet,
                                            protected val factorM: Array[Int],
                                            val field: P) extends Signature[P] {

  def get: BigInt = {
    //Create a map of factors f to their multipliers m (Multiset)

    //Create lists of m factors f. Flatten to single large list of factors

    ???
  }

  def getFactors: List[Int] = ???

}

case object Signature0 extends Signature[P._1.type] {
  override protected def factors: BitSet = BitSet.empty

  override protected def factorM: Array[Int] = Array.empty[Int]

  val get = BigInt(1)

  val getFactors = List.empty[Int]

  val field = P._1

  override def hashCode(): Int = 1.##
}

case class Signature3()

object Signature {

  //apply

  //forAddition(Edge + Graph)

  //fromFactors

  //Implicit class on Seq of Int providing toSignature syntax

  //Eq instance

  //Monoid instance

  //Field type

  //Field instances for 1(?),2,3,7,13,31,61,127,251,384,509,631,761,887,1021,1115,1279
}
