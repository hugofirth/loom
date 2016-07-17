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

import language.higherKinds
import scala.concurrent._
 
import org.gdget.data.query._
import org.gdget.Edge
import org.gdget.partitioned._

import cats._
import cats.std.all._
import cats.syntax.semigroup._

 /** Experiment trait to hold implementation common to all experiments (e.g. IO).
   *
   */
sealed trait Experiment[S[_], V, E[_]] {

  /** Type Aliases for conciceness and clarity */
  //Note that queries needn't return a list of edges, but as we need a fixed return type this seems the most flexible?
  type Q = QueryIO[LogicalParGraph[S, ?, ?[_]], V, E, List[E[V]]]
  type QStream = Stream[Q]

  /** Typeclass instances for S & E */
  implicit def S: ParScheme[S]
  implicit def E: Edge[E]

  /** The graph over which the experiments are run */
  def g: LogicalParGraph[S, V, E]

  /** Map of query idenifiers to queries themselves (QueryIO objects) */
  def queries: Map[String, Q]

  /** Stream of queries made up from the values from `queries`.
    * 
    * The frequency of each distinct query pattern may change over time.
    */
  def queryStream: QStream

  /** Takes any function from a graph to a return type A, and lazily measures the execution
    * time in milliseconds, returning a function from a graph to a tuple (Long, A).
    *
    * TODO: use cats.Eval here
    */
  def time[A](f: LogicalParGraph[S, V, E] => A): LogicalParGraph[S, V, E] => (Long, A) = { g =>
    val t = System.nanoTime
    ((System.nanoTime-t)/1000, f(g))
  }
  
  /** Key method of the Experiment trait which takes a number of queries from `queryStream` to run over `g`.
    *
    * Returns a Future of the Experiment result.
    *  
    * As queries in the workload are read only, individual queries may be evaluated in their own threads. The Future
    * result of each is then combined and returned as Future[A: Monoid] has a Monoid.
    */ 
  def run(n: Int): Future[Experiment.Result] = {
    import ExecutionContext.Implicits.global
    import Experiment._
    import LogicalParGraph._

    val resultStream = queryStream.take(n).map { q => 
      val timedQ = time { graph =>     
        val interpreter = countingInterpreterK[Future, S, V, E]
        val query = q.transKWith[Future](interpreter).run(graph)
        //May seem weird to map the Future then not use the query result, but map is run against the *successful* result
        // of the future, therefore when the map function is executed, the mutable interpreter's iptCount is guaranteed
        // to be updated. Icky I know - will all change when able to return iptCount with query result.
        query.map(_ => interpreter.iptCount)
      } (g)
      timedQ._2.map(ipt => Result(timedQ._1, ipt))
    }
    resultStream.reduceLeft(_ |+| _) 
  }

}

object Experiment {
  //Result case class, which takes Time and IPT at a minimum
  case class Result(time: Long, ipt: Int)

  //Monoid for Result case class
  implicit val resultMonoid: Monoid[Result] = new Monoid[Result] {
    override def empty: Result = Result(0, 0)

    override def combine(x: Result, y: Result): Result = x.copy(time = x.time+y.time, ipt = x.ipt+y.ipt)
  }
}

case class MusicBrainzExperiment[S[_], V, E[_]](g: LogicalParGraph[S, V, E])(implicit val S: ParScheme[S], val E: Edge[E])
  extends Experiment[S, V, E] {

  val q1 = ???

  val q2 = ???

  val q3 = ???
}


