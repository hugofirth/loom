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

/** Description of Class
  *
  * @author hugofirth
  */
package object util {

  /** Method execution timer. Different than timer in Experiment, as this timer is not graph specific nor lazy */
  def time[A](f: => A): (Long, A) = {
    val t = System.nanoTime
    val result = f
    val elapsed = (System.nanoTime-t)/1000000
    (elapsed, result)
  }

}
