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

import scala.concurrent.Future
 
import org.gdget.data.query._
import org.gdget.Edge
import org.gdget.partitioned._

 /** Experiment trait to hold implementation common to all experiments (e.g. IO).
   *
   */
sealed trait Experiment[S[_], V, E[_]] {

  /** Type Aliases for conciceness and clarity */
  type Q = QueryIO[LogicalParGraph[S, ?, ?[_]], V, E, _]
  type QStream = Stream[Q]

  /** Typeclass instances for S & E */
  implicit def S: ParScheme[S]
  implicit def E: Edge[E]

  /** Map of query idenifiers to queries themselves (QueryIO objects) */
  def queries: Map[String, Q]

  /** Stream of queries made up from the values from `queries`.
    * 
    * The frequency of each distinct query pattern may change over time.
    */
  def queryStream: QStream

  /** Takes any function from a graph to a return type A, and lazily measures the execution
    * time, returning a function from a graph  to a tuple (Int, A). 
    *
    * TODO: use cats.Eval here
    */
  def time[A](f: (LogicalParGraph[S, V, E], QStream) => A): (LogicalParGraph[S, V, E], QStream) => (Int, A)
  
  /** Key method of the Experiment trait which takes a Graph object and retuns a Future of the Experiment result.
    *  
    * As queries in the workload are read only, individual queries may be evaluated in their own threads. The Future
    * result of each is then combined and returned as Future[A: Monoid] has a Monoid.
    */ 
  def run(g: LogicalParGraph[S, V, E]): Future[Experiment.Result] 
}

object Experiment {
  //Result case class, which takes Time and IPT at a minimum
  case class Result(time: Int, ipt: Int)
}

