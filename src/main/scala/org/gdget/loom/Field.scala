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

/** Trait for field type, essentially type level representations of a number of small primes and 1. The choice of
  * primes is essentially the largest prime which will can be represented by a bitset of 4,8,16,32 and 128 bits, then
  * every 128 bits after that. The largest prime we consider is 1279, which can be represented by a bitset with 1280
  * bits (bitsets are internally represented as Array[Long], so size must be a multiple of 64 bits).
  *
  * The choice of 1279 has the convenient property that 1279 pow 3 is less than the max value of a signed integer. As
  * we often deal with signatures of 3 factors, being certain that the product of the factors is an Int (rather than a
  * BigInt) is useful for optimisation.
  *
  */
sealed trait Field {
  def value: Int
}

object Field {

  private val fieldPrimes = Map(
    1 -> P._1,
    2 -> P._2,
    3 -> P._3,
    7 -> P._7,
    13 -> P._13,
    31 -> P._31,
    61 -> P._61,
    127 -> P._127,
    251 -> P._251,
    384 -> P._384,
    509 -> P._509,
    631 -> P._631,
    761 -> P._761,
    887 -> P._887,
    1021 -> P._1021,
    1151 -> P._1151,
    1279 -> P._1279
  )

  def apply(p: Int): Field = {
    fieldPrimes.getOrElse(p,
      throw new IllegalArgumentException(s"Provided value $p is not one of our supported Field Primes" +
        s": ${fieldPrimes.keySet}"))
  }

  implicit final class ModdableInt(val n: Int) extends AnyVal {

    /** Method to mod an int by a field (mod not remainder!), plus 1 (our finite field doesn't include 0) and
      * return a short, because maximum size of a number mod a field is 1279.
      */
    def mod(p: Field): Short = ((n.abs % p.value) + 1).toShort

  }
}

object P {

  case object _1 extends Field {
    val value = 1
  }
  case object _2 extends Field {
    val value = 2
  }
  case object _3 extends Field {
    val value = 3
  }
  case object _7 extends Field {
    val value = 7
  }
  case object _13 extends Field {
    val value = 13
  }
  case object _31 extends Field {
    val value = 31
  }
  case object _61 extends Field {
    val value = 61
  }
  case object _127 extends Field {
    val value = 127
  }
  case object _251 extends Field {
    val value = 251
  }
  case object _384 extends Field {
    val value = 384
  }
  case object _509 extends Field {
    val value = 509
  }
  case object _631 extends Field {
    val value = 631
  }
  case object _761 extends Field {
    val value = 761
  }
  case object _887 extends Field {
    val value = 887
  }
  case object _1021 extends Field {
    val value = 1021
  }
  case object _1151 extends Field {
    val value = 1151
  }
  case object _1279 extends Field {
    val value = 1279
  }
}

