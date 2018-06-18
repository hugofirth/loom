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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
  protected def factorMultiSet: Seq[Int]

  /** Get the product of all factors in this signature, as a BigInt */
  def value: BigInt

  /** Get the list of all factors in this signature */
  def factors: List[Int]

  def canEqual(other: Any): Boolean = other.isInstanceOf[Signature[_]]

  /** At the moment, this equals method does not account for Field, I should fix that! Even though the typesafe ===
    * won't have this problem
    */
  override def equals(other: Any): Boolean = other match {
    case that: Signature[_] =>
      (this eq that) ||
        (that canEqual this) &&
          factorSet == that.factorSet &&
          factorMultiSet == that.factorMultiSet
    case _ => false
  }

  override def hashCode(): Int = {
    37 * ((31 * factorSet.hashCode()) + factorMultiSet.hashCode())
  }
}



object Signature {

  def apply[P <: Field](p: P, factors: Int*): Signature[P] = fromFactors(p, factors:_*)

  /** Return empty Zero signature */
  def zero[P <: Field]: Signature[P] = Signature0[P]

  private final case class SignatureN[P <: Field] (protected val factorSet: BitSet,
                                                   protected val factorMultiSet: Seq[Int],
                                                   field: P) extends Signature[P] {

    def value: BigInt = factors.foldLeft(BigInt(1)) { _ * _ }

    def factors: List[Int] = {
      //Create a map of factors f to their multipliers m (Multiset)
      val factors = factorMultiSet.map(p => (p >> 16) -> p.toShort)
      //Create lists of m factors f. Flatten to single large list of factors
      factors.toList.flatMap(p => List.fill(p._2)(p._1))
    }

    override def toString: String = s"Signature($value, Z${field.value}, $factorMultiSet)"

  }

  private case object Signature0 extends Signature[Nothing] {

    protected val factorSet: BitSet = BitSet(1)

    protected val factorMultiSet: Seq[Int] = Array[Int]((1 << 16) | 1)

    val value = BigInt(1)

    val factors = List.empty[Int]

    @inline
    final def apply[P <: Field] = this.asInstanceOf[Signature[P]]

    @inline
    final def unapply[P <: Field](other: Signature[P]) = this eq other

    override def toString: String = s"Signature($value, Z?, $factorMultiSet)"
  }

  private final case class Signature3[P <: Field] (f1: Short, f2: Short, f3: Short, field: P) extends Signature[P] {

    //TODO: Why aren't all these fields lazy vals?

    //TODO: Work out why this was a sorted set rather than a bitset?
    /** The set of distinct factors which make up this graph signature */
    protected lazy val factorSet: SortedSet[Int] = BitSet(f1, f2, f3)

    /** The factor multipliers, or number of times each factor appears in the signature, bit packed into an array of Ints
      *
      * Effectively a Map[Int, Int] where each Int -> Int entry is represented by a single Int (because each factor and its
      * multiplier are guaranteed to take less than 2 bytes to represent)
      */
    protected lazy val factorMultiSet: Seq[Int] = {
      //This seems horrendously over the top premature optimisation for a little space saving. Are we not compute bound anyway?
      List(f1, f2, f3).sorted.groupBy(identity).map(p => (p._1 << 16) | p._2.size).toArray
    }

    /** Get the product of all factors in this signature, as a BigInt */
    override def value: BigInt = f1 * f2 * f3

    /** Get the list of all factors in this signature */
    override def factors: List[Int] = List(f1, f2, f3)

    override def toString: String = s"Signature($value, Z${field.value}, $factorMultiSet)"

    /** Breaking the equals contract ... just a little
      */
    override def equals(other: Any): Boolean = other match {
      case that: Signature3[_] =>
        (this eq that) || this.value == that.value
      case that: Signature[_] =>
        (this eq that) ||
          (that canEqual this) &&
            factorSet == that.factorSet &&
            factorMultiSet == that.factorMultiSet
      case _ => false
    }
  }

  def forAdditionToEdges[V: Labelled, E[_]: Edge, P <: Field](added: E[V], context: Set[E[V]],
                                                              field: P): Signature[P] = {

    val (l, r) = Edge[E].vertices(added)
    //Work out existing degree for l & r
    val lDeg = context.count(e => Edge[E].left(e) == l || Edge[E].right(e) == l)
    val rDeg = context.count(e => Edge[E].left(e) == r || Edge[E].right(e) == r)

    //Calculate's edge factor and degree factors for l & r
    val rDegFactor = (Labelled[V].label(r) + (rDeg + 1)) mod field
    val lDegFactor = (Labelled[V].label(l) + (lDeg + 1)) mod field
    //TOOD: Mod the subtraction if edges undirected
    val eFactor = (Labelled[V].label(l) - Labelled[V].label(r)) mod field

    //Lets sort the factors - yay for pattern matching assignment
//    val List(_1, _2, _3) = List(rDegFactor, lDegFactor, eFactor).sorted

    Signature3(rDegFactor, lDegFactor, eFactor, field)
  }

  def forAdditionToGraph[G[_, _[_]], V: Labelled, E[_]: Edge, P <: Field](added: E[V], context: G[V, E], field: P)
                                                                  (implicit gEv: Graph[G, V, E]): Signature[P] = {

    forAdditionToEdges(added, Graph[G, V, E].edges(context).toSet, field)
  }

  def fromFactors[P <: Field](p: P, factors: Int*): Signature[P] = {
    //Ensure correct by construction
    require(factors.forall(f => f <= p.value && f > 0), s"All factors must be within specified finite field [1,${p.value}")

    //TODO: Is this guaranteed (or even likely) to be sorted?
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
    override def empty: Signature[P] = Signature.zero[P]

    /** The below is not very DRY, but should be fairly efficient. Need benchmarks */
    override def combine(x: Signature[P], y: Signature[P]): Signature[P] = (x, y) match {
      case (l, Signature0()) => l
      case (Signature0(), r) => r
      case (SignatureN(lfSet, lfMultiSet, p), r) =>
        //Compute the union between the two factor sets
        val fSUnion = lfSet | r.factorSet
        //Compute the multiSet
        val lMs = lfMultiSet.map(e => (e >> 16) -> e.toShort)
        val rMs = r.factorMultiSet.map(e => (e >> 16) -> e.toShort)

        //Can get away with that last bitwise or because we know that the reduce operation will never produce an int of
        // more than 16 bits
        val multiSetUnion = (lMs.toBuffer ++= rMs).sortBy(_._1).groupBy(_._1).mapValues({ e =>
          e.foldLeft(0) { (acc, entry) => acc + entry._2 }
        }).map(e => (e._1 << 16) | e._2)

        //Create the new signatureN
        SignatureN(fSUnion, multiSetUnion.toArray, p)
      case (l, SignatureN(rfSet, rfMultiSet, p)) =>
        //Compute the union between the two factor sets
        val fSUnion = rfSet | l.factorSet
        //Compute the multiSet
        val rMs = rfMultiSet.map(e => (e >> 16) -> e.toShort)
        val lMs = l.factorMultiSet.map(e => (e >> 16) -> e.toShort)

        //Can get away with that last bitwise or because we know that the reduce operation will never produce an int of
        // more than 16 bits
        val multiSetUnion = (lMs.toBuffer ++= rMs).sortBy(_._1).groupBy(_._1).mapValues({ e =>
          e.foldLeft(0) { (acc, entry) => acc + entry._2 }
        }).map(e => (e._1 << 16) | e._2)

        //Create the new signatureN
        SignatureN(fSUnion, multiSetUnion.toArray, p)
      case (Signature3(lf1, lf2, lf3, p), Signature3(rf1, rf2, rf3, _)) =>
        //Compute the union between the two factor sets
        val facs = List(lf1.toInt, lf2.toInt, lf3.toInt, rf1.toInt, rf2.toInt, rf3.toInt)
        val fSUnion = BitSet() ++ facs

        //Compute the multiSet
        val multiSetUnion = facs.groupBy(identity).map(p => (p._1 << 16) | p._2.size)

        //Create the new signatureN
        SignatureN(fSUnion, multiSetUnion.toArray, p)
    }
  }

}
