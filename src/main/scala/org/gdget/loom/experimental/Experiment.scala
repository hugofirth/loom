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

 /** Experiment trait to hold implementation common to all experiments (e.g. IO).
   *
   */
sealed trait Experiment[S[_], V, E[_]] {

  /** Typeclass instances for S & E */
  implicit def S: ParScheme[S]
  implicit def E: Edge[E]

  /** Map of query idenifiers to queries themselves (QueryIO objects) */
  def queries: Map[String, QueryIO[LogicalParGraph[S, ?, ?[_]], V, E, _]]

  /** Stream of queries made up from the values from `queries`.
    * 
    * The frequency of each distinct query pattern may change over time.
    */
  def queryStream: Stream[QueryIO[LogicalParGraph[S, ?, ?[_]], V, E, _]] 

  
  //time function: ((G, Q) => A): (Int, A) ... or Make an "Instrumented" Applicative?
  
  //run function which produces a result.... where are we doing IO?
  
}

object Experiment {
  //Result case class, which takes Time and IPT at a minimum
}

