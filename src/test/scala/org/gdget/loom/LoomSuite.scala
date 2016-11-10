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

import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.prop.{Configuration, GeneratorDrivenPropertyChecks}
import org.typelevel.discipline.scalatest.Discipline

/** Base spec for property based tests of Loom code.
  *
  */
trait LoomSuite extends FunSuite
  with BeforeAndAfterAll with Matchers with GeneratorDrivenPropertyChecks with Discipline with Configuration {

  /** Configure the ScalaCheck */
  implicit override val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = 100)
}
