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

/** Testing the functionality of objects of the TPSTry class.
  *
  */
class TPSTrySpec extends LoomSuite{

  //TODO: Test pruning

  //TODO: Test after adding a single edge graph to the TPSTry its factor exists as a child of the root.

  //TODO: Test that after the addition of a single edge graph 5 times, the support of that child node is 5.

  //TODO: Test that TPSTry is properly immutable. Try adding or removing something from one created from another.

  //TODO: Add a succession of paths and check that the support for each is lower than the parent

  //TODO: Add a path and check withFactors.

  //TODO: Recursively check that only Root or Tip nodes have no children

  //TODO: Look into property testing with random connected sets of graphs. Highly hetero with small fields should
  // lead to collisions. Highl homo should lead to collisions

}
