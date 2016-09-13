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
import jawn.ast.JValue
import org.gdget.Edge
import org.gdget.data.UNeighbourhood
import org.gdget.loom.GraphReader
import org.gdget.loom.experimental.ProvGen.{Agent, Activity, Entity, Vertex => ProvGenVertex}

import scala.language.higherKinds

/** Entry point for the Loom experiments
  *
  * @author hugofirth
  */
object Main {

  case class Config(dfs: String, bfs: String, rand: String, stoch: String, numK: List[Int])


  def main(args: Array[String]): Unit = {

  }

  def jsonToNeighbourhood[V, E[_]: Edge](jValue: JValue): String Xor UNeighbourhood[V, E] = {

    val getId = (j: JValue) => j.get("id").getLong
      .fold(Xor.left[String, Long]("Unable to properly parse vertex id"))(Xor.right[String, Long])

    val getV = (j: JValue, id: Int) => j.get("label").getString
      .fold(Xor.left[String, ProvGenVertex]("Unable to properly parse vertex label")) {
        case "AGENT" => Xor.right[String, ProvGenVertex](Agent(id, None))
        case other => Xor.left[String, ProvGenVertex](s"Unrecognised vertex label $other")
      }
    //Format of each entry in Json array: {id, label, in[{neighbourId, edgeLabel}, ...], out[{}, ...]}

    val interim = for {
      id <- getId(jValue)
      center <- getV(jValue, id.toInt)
    } yield center

    val n = UNeighbourhood()


  }

  def provGenExperiment(conf: Config): String = {

    //ProvGen graph stream json dumps
    val dfs = Paths.get(conf.dfs)
    val bfs = Paths.get(conf.bfs)
    val rand = Paths.get(conf.rand)
    //val stoch = Paths.get(conf.stoch)

    //For each stream order
    List(dfs, bfs, rand).map { order =>
      //Get json dump
      GraphReader.read(order)

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
