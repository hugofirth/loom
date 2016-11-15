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
import org.gdget.{Edge, HPair}
import org.gdget.partitioned._
import org.gdget.partitioned.data._
import org.gdget.loom.experimental.ProvGen.{ProvGenExperimentMeta, Vertex => ProvGenVertex}
import org.gdget.loom.experimental.MusicBrainz.{MusicBrainzExperimentMeta, Vertex => MusicBrainzVertex}
import cats._
import cats.instances.all._
import cats.syntax.semigroup._
import org.apache.commons.math3.random.{JDKRandomGenerator, RandomDataGenerator}
import org.gdget.data.SimpleGraph

import scala.util.Random


 /** Experiment trait to hold implementation common to all experiments (e.g. IO).
   *
   */
sealed trait Experiment[V, E[_]] {


  import Experiment._

  /** Typeclass instances for V & E */
  implicit def E: Edge[E]
  implicit def V: Partitioned[V]

//  /** The graph over which the experiments are run */
//  def g: LogicalParGraph[V, E]


  /** The Seed Int to pass to random generators, making experiments truly repeatable */
   def seed: Int

  /** Map of query idenifiers to queries themselves (QueryIO objects) */
  def queries: Map[String, (Q[V, E], SimpleGraph[V, E])]

  /** Stream of queries made up from the values from `queries`.
    *
    * The relative frequency of each distinct query pattern changes over time in a periodic, repeating fashion which
    * simulates a pseudo-realistic pattern of workload change. If you plot the frequencies of a given query over time it
    * will resemble a sine wave.
    */
  def periodicQueryStream: QStream[V, E] = {
    def selectRange(ranges: Vector[(String, Double, Double)], offset: Double, value: Double) = {
      //Make sure offset is < 7
      val effectiveOffset = offset % 7
      //Add offset to value
      //Box value in -3.5,3.5 range
      val effective = if(value + effectiveOffset >= 3.5) (value + effectiveOffset) - 7 else value + effectiveOffset
      //Find the range which the effective value falls within. Note that ranges are min inclusive max exclusive
      ranges.find { case (key, min, max) => effective >= min && effective < max } map(_._1)
    }

    //Create a vector of "Ranges" (i.e. (Double, Double)), one for each value in queries map.
    //A range of 7 around 0 encompasses ~100% of values in a standard normal distribution
    val rangeSize = 7/queries.size
    //Fold over queries to construct ranges
    val (_, ranges) = queries.foldLeft((-3.5, Vector.empty[(String, Double, Double)])) { (acc, entry) =>
      val (floor, rs) = acc
      val key = entry._1
      (floor + rangeSize, rs :+ ((key, floor, floor+rangeSize)))
    }

    //Mutability, but its all method local.
    //Not too worried about randomness proprties, so JDK Random should suffice.
    // http://commons.apache.org/proper/commons-math/userguide/random.html is an interesting read though.
    val rand = new JDKRandomGenerator(seed)
    val rng = new RandomDataGenerator(rand)

    //Create stream of doubles which increment by offset
    val offsets = Stream.from(0).map(_.toDouble/10)
    //map over lazy stream, identifying queries for each new random value.
    offsets.flatMap(o => selectRange(ranges, o, rng.nextGaussian(0, 1))).flatMap(queries.get)
  }


  /** Stream of queries made up from the values from `queries`.
    *
    * The relative frequencies of each distinct query are fixed according to the map of query identifiers to doubles passed
    * in as a method parameter
    */
  def fixedQueryStream(frequencies: Map[String, Double]): QStream[V, E] = {
    require((frequencies.keySet &~ queries.keySet).isEmpty, "The frequencies Map must include entries for every query!")
    require(frequencies.values.reduceLeftOption(_ + _).getOrElse(0D) > 0.99, "Frequencies must sum to ~ 1.0!")

    val rand = new Random(seed)
    val randStream = Stream.continually(rand.nextDouble())

    //Calculate distinct upper bounds for each queries frequency and invert Map
    //Eg Map("a"->0.2, "b"->0.3, "c"->0.4, "d"->0.1) becomes List(0.2->"a", 0.5->"b", 0.9->"c", 1.0->"d")
    val freqUpBounds = frequencies.toList.foldLeft(List.empty[(String, Double)]) { (acc, elem) =>
      val upBound = acc.headOption.map(_._2).getOrElse(0D) + elem._2
      (elem._1, upBound) :: acc
    }.reverse

    //As the bounds are sorted in ascending order by construction, the first bound which is greater than the random Double
    //represents the query string for that Stream element.
    //Sweeping some assumptions under the covers with these two flatmaps - make more robust!
    randStream.flatMap(r => freqUpBounds.find(r <= _._2)).flatMap(q => queries.get(q._1))
  }

  /** Takes any function from a graph to a return type A, and lazily measures the execution
    * time in milliseconds, returning a function from a graph to a tuple (Long, A).
    *
    * TODO: use cats.Eval here
    */
  def time[A](f: LogicalParGraph[V, E] => A): LogicalParGraph[V, E] => (Long, A) = { g =>
    val t = System.nanoTime
    val result = f(g)
    ((System.nanoTime-t)/1000000, result)
  }
  
  /** Key method of the Experiment trait which takes a number of queries from `queryStream` to run over `g`.
    *
    * Returns a Future of the Experiment result.
    *  
    * As queries in the workload are read only, individual queries may be evaluated in their own threads. The Future
    * result of each is then combined and returned as Future[A: Monoid] has a Monoid.
    */ 
  def run(n: Int, qs: QStream[V, E], g: LogicalParGraph[V, E]): Future[Experiment.Result] = {
    import ExecutionContext.Implicits.global
    import Experiment._
    import LogicalParGraph._

    val resultStream = qs.take(n).map { case (q ,gq) =>
      val timedQ = time { graph =>     
        val interpreter = countingInterpreterK[Future, V, E]
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

  /** Simple method to test each of the specified queries in the experiment against the experiment's graph and print
    * the results to std out (should probably make this a bit better, but needs must when the devil calls)
    */
  def trial(g: LogicalParGraph[V, E]): Unit = queries.foreach { case (key, (q, gq)) =>
    println(s"Testing query $key")
    val result = q.transK[Id].run(g)
    println(s"Printing result $result")
  }

 }

object Experiment {

  /** Type Aliases for conciceness and clarity */
  //Note that we don't specify a return type for the queries, because for this experiment we don't care about getting more
  // than metadata for the execution of each query from the interpreter. If was wanted to use the results we would have
  // have to do something smarter here, or just fix the query return type.
  type Q[V, E[_]] = QueryIO[LogicalParGraph, V, E, _]
  type QStream[V, E[_]] = Stream[(Q[V, E], SimpleGraph[V, E])]

  /** Result case class, which takes Time and IPT at a minimum */
  case class Result(time: Long, ipt: Int)

  /** Monoid for Result case class */
  implicit val resultMonoid: Monoid[Result] = new Monoid[Result] {
    override def empty: Result = Result(0, 0)

    override def combine(x: Result, y: Result): Result = x.copy(time = x.time+y.time, ipt = x.ipt+y.ipt)
  }

  /** Types in the Experiment ADT */
  case class ProvGenExperiment(seed: Int)
                              (implicit val E: Edge[HPair],
                               val V: Partitioned[ProvGenVertex]) extends Experiment[ProvGenVertex, HPair] with ProvGenExperimentMeta


  case class MusicBrainzExperiment(seed: Int)
                                  (implicit val E: Edge[HPair],
                                   val V: Partitioned[MusicBrainzVertex]) extends Experiment[MusicBrainzVertex, HPair] with MusicBrainzExperimentMeta

  //case object DBLPExperiment
  //case object DBPediaExperiment
}


/** Utility trait to be extended for each experiment to provide a "Meta" trait which carries all experiment info (like
  * dataset and workload queries) and may be mixed back into the Experiment ADT objects above.
  */
trait ExperimentMeta[V, E[_]] { self: Experiment[V, E] =>

  import Experiment._

  def queries: Map[String, (Q[V, E], SimpleGraph[V, E])]
}


