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

import org.gdget.partitioned.PartId

/** A generic trait for functionality common to the Vertex ADTs for each dataset used in a Loom experiment (e.g. ProvGen)
  *
  * Note that these vertices are partitioned, i.e. may have a PartId for the partition in which they reside.
  *
  * Each ADT will currently have to provide its own typeclass instances, though this could potentially be brought up here.
  */
trait GenVertex {
  def id: Int

  def part: Option[PartId]

  def canEqual(other: Any): Boolean

  override def equals(other: Any): Boolean = other match {
    case that: GenVertex =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = id.hashCode
}
