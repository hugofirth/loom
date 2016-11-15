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

/** Simple Vertex ADT for use in tests */
sealed trait TestVertex {
  def a: Int
}

case class A(a: Int) extends TestVertex
case class B(a: Int) extends TestVertex
case class C(a: Int) extends TestVertex
case class D(a: Int) extends TestVertex
case class E(a: Int) extends TestVertex
case class F(a: Int) extends TestVertex
case class G(a: Int) extends TestVertex
case class H(a: Int) extends TestVertex
case class I(a: Int) extends TestVertex
case class J(a: Int) extends TestVertex
case class K(a: Int) extends TestVertex
case class L(a: Int) extends TestVertex
case class M(a: Int) extends TestVertex
case class N(a: Int) extends TestVertex
case class O(a: Int) extends TestVertex
//case class P(a: Int) extends TestVertex
case class Q(a: Int) extends TestVertex
case class R(a: Int) extends TestVertex
case class S(a: Int) extends TestVertex
case class T(a: Int) extends TestVertex
case class U(a: Int) extends TestVertex
case class V(a: Int) extends TestVertex
case class W(a: Int) extends TestVertex
case class X(a: Int) extends TestVertex
case class Y(a: Int) extends TestVertex
case class Z(a: Int) extends TestVertex


object TestVertex {

}
