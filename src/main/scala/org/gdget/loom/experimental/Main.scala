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

import java.nio.file.Paths

import cats.data.Xor
import cats.instances.all._
import cats._
import cats.syntax.all._
import jawn.ast.JValue
import org.gdget.{Edge, HPair}
import org.gdget.data.UNeighbourhood
import org.gdget.loom.GraphReader
import org.gdget.loom.experimental.ProvGen.{Activity, Agent, Entity, Vertex => ProvGenVertex}
import org.gdget.std.all._

import scala.language.higherKinds

/** Entry point for the Loom experiments
  *
  * @author hugofirth
  */
object Main {

  case class Config(dfs: String, bfs: String, rand: String, stoch: String, numK: List[Int])


  def main(args: Array[String]): Unit = {

  }

  //TODO: Abstract away from ProvGen
  def jsonToNeighbourhood(jValue: JValue): String Xor UNeighbourhood[ProvGenVertex, HPair] = {

    sealed trait Direction
    case object In extends Direction
    case object Out extends Direction

    def getId(j: JValue) = {
      val idOpt = j.get("id").getLong
      idOpt.fold(Xor.left[String, Long]("Unable to properly parse vertex id")) { Xor.right[String, Long] }
    }

    def getCenter(j: JValue, id: Int) = {
      val lblOpt = j.get("label").getString
      lblOpt.fold(Xor.left[String, ProvGenVertex]("Unable to properly parse vertex label")) {
        case "AGENT" => Xor.right[String, ProvGenVertex](Agent(id, None))
        case "ACTIVITY" => Xor.right[String, ProvGenVertex](Activity(id, None))
        case "ENTITY" => Xor.right[String, ProvGenVertex](Entity(id, None))
        case other => Xor.left[String, ProvGenVertex](s"Unrecognised vertex label $other")
      }
    }

    def getNeighbour(j: JValue, d: Direction) = {
      val neighbourId = getId(j)
      neighbourId.flatMap {id =>
        val lblOpt = j.get("label").getString
        lblOpt.fold(Xor.left[String, ProvGenVertex]("Unable to properly parse edge label")) {
          case "WASDERIVEDFROM" => Xor.right[String, ProvGenVertex](Entity(id.toInt, None))
          case "WASGENERATEDBY" =>
            Xor.right[String, ProvGenVertex](if(d == Out) Entity(id.toInt, None) else Activity(id.toInt, None))
          case "WASASSOCIATEDWITH" =>
            Xor.right[String, ProvGenVertex](if(d == Out) Activity(id.toInt, None) else Agent(id.toInt, None))
          case "USED" =>
            Xor.right[String, ProvGenVertex](if(d == Out) Activity(id.toInt, None) else Entity(id.toInt, None))
          case other => Xor.left[String, ProvGenVertex](s"Unrecognised edge label $other")
        }
      }
    }

    def getNeighbours(j: JValue, d: Direction) = {
      //Does it matter if we go toList then sequence or sequence map toList ?
      Stream.from(0).map(j.get).takeWhile(_.nonNull).map(getNeighbour(_, d)).toList.sequence
    }

    //Format of each entry in Json array: {id, label, in[{neighbourId, edgeLabel}, ...], out[{}, ...]}

    //TODO: Why are we using Set of unit rather than empty Set here?
    for {
      id <- getId(jValue)
      center <- getCenter(jValue, id.toInt)
      inN <- getNeighbours(jValue.get("in"), In)
      outN <- getNeighbours(jValue.get("out"), Out)
      edges = Set(())
    } yield UNeighbourhood[ProvGenVertex, HPair](center, inN.map(_ -> edges).toMap, outN.map(_ -> edges).toMap)

  }

  def provGenExperiment(conf: Config): String = {


    //For each stream order
    List(conf.dfs, conf.bfs, conf.rand).map { order =>
      //Get json dump
      //map to right - find someway of passing fail forward in the event of a left
      val nStream = GraphReader.read(order, jsonToNeighbourhood)

    }

      //Get json dump - create Stream[UNeighbourhood[ProvGenVertex, HPair]] with GraphReader

      //In order to to do ^^ we will need to define a function which takes a JValue for each elem of the adjList JSon
      // and returns parses it to return a Uneighbourhood

      //Given Stream[Neighbourhood], for each partitioner:

        //Consume Stream[Neighbourhood] to produce g: LogicalParGraph[ProvGenVertex, HPair]

        //Now that we have g, create ProvGenExperiment instance and run 100 queries with one of the two stream options

        //Note that ^^ requires that we have defined the ProvGen appropriate queries in ProvGenExpMeta

        //Once we have the results (number of traversals and time), log it to console and add to Result string

    //Once we have done this for all combos, return string.

    //May need another method for Loom to easily manage multiple window sizes in experiments
    //May also need to take some kind of config parameter into method for number of partitions.


    ???
  }



}
