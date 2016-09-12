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

/** Main entry point to loom partitioner
  *
  * @author hugofirth
  */
object Main extends App {

  //Would be cool to make this a CLI app with a couple of input modes:
  // Kafka connector? Connector for some other MQs?
  // Read in from a file?
  // Start as a REST service which accepts PUT requests for new edges

  //As for output modes?

  //All this streaming in/out probably needs one of Akka, Monix or FS2

}
