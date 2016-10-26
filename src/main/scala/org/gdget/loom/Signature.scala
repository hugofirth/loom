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

import org.gdget.{Edge, Graph}
import Field._
import cats.kernel.{Eq, Monoid}

import language.higherKinds
import scala.collection.immutable.{BitSet, SortedSet}

/** Trait for objects which represent graph signatures
  *
  * @author hugofirth
  */
sealed trait Signature[P <: Field] {

  /** The set of distinct factors which make up this graph signature*/
  protected def factorSet: SortedSet[Int]

  /** The factor multipliers, or number of times each factor appears in the signature, bit packed into an array of Ints
    *
    * Effectively a Map[Int, Int] where each Int -> Int entry is represented by a single Int (because each factor and its
    * multiplier are guaranteed to take less than 2 bytes to represent)
    */
  protected def factorMultiSet: Array[Int]

  /** The prime p which defines the finite field into which all factors must fall */
  //TODO: Make this an Option. Or remove it.
  def field: Field

  /** Get the product of all factors in this signature, as a BigInt */
  def value: BigInt

  /** Get the list of all factors in this signature */
  def factors: List[Int]

  def canEqual(other: Any): Boolean = other.isInstanceOf[Signature[_]]

  override def toString: String = s"Signature($value)"

  override def equals(other: Any): Boolean = other match {
    case that: Signature[_] =>
      (this eq that) ||
        (that canEqual this) &&
          field == that.field &&
          factorSet == that.factorSet &&
          factorMultiSet.sameElements(that.factorMultiSet)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(factorSet, factorMultiSet)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}



object Signature {

  //apply call fromFactors

  /** Return empty Zero signature */
  def zero[P <: Field]: Signature[P] = Signature0

  private final case class SignatureN[P <: Field] (protected val factorSet: BitSet,
                                                   protected val factorMultiSet: Array[Int],
                                                   field: P) extends Signature[P] {

    //replace with foldLeftOption and then provide a bigInt(1) default
    def value: BigInt = factors.map(BigInt.apply).product

    def factors: List[Int] = {
      //TODO: make the below faster by only using MultiSet, Set is just for equality checks.
      //Create a map of factors f to their multipliers m (Multiset)
      val factors = factorSet.toList.zip(factorMultiSet).map(p => (p._1, p._2.toShort))
      //Create lists of m factors f. Flatten to single large list of factors
      factors.flatMap(p => List.fill(p._2)(p._1))
    }

  }

  private case object Signature0 extends Signature[P._1.type] {

    protected val factorSet: BitSet = BitSet.empty

    protected val factorMultiSet = Array.empty[Int]

    val value = BigInt(1)

    val factors = List.empty[Int]

    val field = P._1

  }

  private final case class Signature3[P <: Field] (f1: Short, f2: Short, f3: Short, field: P) extends Signature[P] {

    /** The set of distinct factors which make up this graph signature */
    protected def factorSet = SortedSet(f1, f2, f3)

    /** The factor multipliers, or number of times each factor appears in the signature, bit packed into an array of Ints
      *
      * Effectively a Map[Int, Int] where each Int -> Int entry is represented by a single Int (because each factor and its
      * multiplier are guaranteed to take less than 2 bytes to represent)
      */
    protected def factorMultiSet: Array[Int] = {
      //This seems horrendously over the top premature optimisation for a little space saving. Are we not comput bound anyway?
      List(f1, f2, f3).sorted.groupBy(identity).map(p => (p._1 << 16) | p._2.size).toArray
    }

    /** Get the product of all factors in this signature, as a BigInt */
    override def value: BigInt = f1 * f2 * f3

    /** Get the list of all factors in this signature */
    override def factors: List[Int] = List(f1, f2, f3)
  }

  def forAdditionToEdges[V: Labelled, E[_]: Edge, P <: Field](added: E[V], context: Set[E[V]],
                                                              field: P): Signature[P] = {

    val (l, r) = Edge[E].vertices(added)
    //Work out existing degree for l & r
    val lDeg = context.count(e => Edge[E].left(added) == l || Edge[E].right(added) == l)
    val rDeg = context.count(e => Edge[E].left(added) == r || Edge[E].right(added) == r)

    //Calculate's edge factor and degree factors for l & r
    val rDegFactor = (Labelled[V].label(r) + (rDeg + 1)) mod field
    val lDegFactor = (Labelled[V].label(l) + (lDeg + 1)) mod field
    val eFactor = (Labelled[V].label(l) - Labelled[V].label(r)) mod field
    Signature3(rDegFactor, lDegFactor, eFactor, field)
  }

  def forAdditionToGraph[G[_, _[_]], V: Labelled, E[_]: Edge, P <: Field](added: E[V], context: G[V, E], field: P)
                                                                  (implicit gEv: Graph[G, V, E]): Signature[P] = {

    forAdditionToEdges(added, Graph[G, V, E].edges(context).toSet, field)
  }

  def fromFactors[P <: Field](p: P, factors: Int*): Signature[P] = {
    //Ensure correct by construction
    require(factors.forall(f => f <= p.value && f > 0), s"All factors must be within specified finite field [1,${p.value}")

    val factorsGrouped = factors.groupBy(identity)
    val factorSet = BitSet() ++ factorsGrouped.keySet
    val factorMultiSet = factorsGrouped.map(p => (p._1 << 16) | p._2.size).toArray
    SignatureN(factorSet, factorMultiSet, p)
  }

  //Eq instance
  implicit def signatureEq[P <: Field] = new Eq[Signature[P]] {
    /** Can defer down to normal equals as that is efficient and this gives us type safety */
    override def eqv(x: Signature[P], y: Signature[P]): Boolean = x == y
  }

  //Monoid instance
  implicit def signatureMonoid[P <: Field] = new Monoid[Signature[P]] {
    // Ok - so we do need Signature0 to be Signature[Nothing] ...
    override def empty: Signature[P] = ???

    override def combine(x: Signature[P], y: Signature[P]): Signature[P] = {
      //This is going to be an important method
      ???
    }
  }

}
