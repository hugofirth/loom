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

import cats.kernel.laws.{GroupLaws, OrderLaws}
import org.scalacheck.{Arbitrary, Cogen, Gen}

/** Testing the functionality of objects of the Signature class.
  *
  * Also using Discipline to check that Signature's typeclass instances obey their respective laws.
  */
class SignatureSpec extends LoomSuite {

  import Signature._

  /** Arbitrary instance for Field type */
  implicit val arbitraryField: Arbitrary[Field] = Arbitrary(
    Gen.oneOf(P._1, P._2, P._3, P._7, P._13, P._31, P._61, P._127,P._251, P._384, P._509, P._631, P._761, P._887,
      P._1021, P._1151, P._1279)
  )

  /** Method to define arbitrary instances for Signature with given fields */
  def arbitrarySignature[P <: Field](p: P): Arbitrary[Signature[P]] = Arbitrary {

    //Generator for a factor
    def genFactor(p: P) = for {
      f <- Gen.choose(1, p.value)
    } yield f

    def genNFactors(p: P, n: Int): Gen[List[Int]] = Gen.listOfN(n, genFactor(p))


    //Simple generator method for Signatures
    def genSig(numFactors: Int, p: P): Gen[Signature[P]] = {
      if(numFactors <= 0)
        Gen.const(Signature.zero[P])
      else {
        for {
          facs <- genNFactors(p, numFactors)
        } yield Signature(p, facs: _*)
      }
    }

    Gen.sized { size =>
      for {
        sig <- genSig(size, p)
      } yield sig
    }
  }

  /** Method to define cogen instances for Signatures for given fields */
  def cogenInst[P <: Field]: Cogen[Signature[P]] = Cogen[List[Int]].contramap(_.factors)

  /** Arbitrary Instances for Signatures with Fields of 1, 13, 251, & 1279 */
  implicit val arbSigZ1 = arbitrarySignature(P._1)
  implicit val arbSigZ13 = arbitrarySignature(P._13)
  implicit val arbSigZ251 = arbitrarySignature(P._251)
  implicit val arbSigZ1279 = arbitrarySignature(P._1279)

  /** Cogen Instances for Signatures with Fields of 1, 13, 251, & 1279 */
  //Be aware - I have no idea what I'm doing
  implicit val cogenSigZ1 = cogenInst[P._1.type]
  implicit val cogenSigZ13 = cogenInst[P._13.type]
  implicit val cogenSigZ251 = cogenInst[P._251.type]
  implicit val cogenSigZ1279 = cogenInst[P._1279.type]


  //Check the Laws
  checkAll("Monoid[Signature[Z1]]", GroupLaws[Signature[P._1.type]].monoid)
  checkAll("Monoid[Signature[Z13]]", GroupLaws[Signature[P._13.type]].monoid)
  checkAll("Monoid[Signature[Z251]]", GroupLaws[Signature[P._251.type]].monoid)
  checkAll("Monoid[Signature[Z1279]]", GroupLaws[Signature[P._1279.type]].monoid)
  checkAll("Eq[Signature[Z1]]", OrderLaws[Signature[P._1.type]].eqv)
  checkAll("Eq[Signature[Z13]]", OrderLaws[Signature[P._13.type]].eqv)
  checkAll("Eq[Signature[Z251]]", OrderLaws[Signature[P._251.type]].eqv)
  checkAll("Eq[Signature[Z1279]]", OrderLaws[Signature[P._1279.type]].eqv)


  //Other tests

  test("A signature may have the same value but different factors, and therefore not be equal") {

    val a = Signature(P._13, 1, 12)
    val b = Signature(P._13, 2, 6)
    val c = Signature(P._13, 3, 4)

    a.value should be (b.value)
    b.value should be (c.value)

    a.factors should not be b.factors
    b.factors should not be c.factors

    a should not equal b
    b should not equal c
  }

  //Create Scalacheck generator for numbers outside of a field
  val invalidFactorFieldPairs = for {
    field <- Arbitrary.arbitrary[Field]
    factor <- Gen.choose(field.value + 1, Int.MaxValue)
  } yield (field, factor)

  test("You may not construct signatures with factors outside of their field")( forAll (invalidFactorFieldPairs) {
    case (field, factor) =>
      assertThrows[IllegalArgumentException] {
        Signature(field, factor)
      }
  })

  //TODO: Remember to test the divisibility functionality when we finish it.

  test("A signature's value must always equal the product of its factors")( forAll { (sig: Signature[P._251.type]) =>
    sig.value should equal (sig.factors.map(BigInt.apply).product)
  })

  //TODO: Test that empty + a-b has the same signature as a-b-c - b-c

}
