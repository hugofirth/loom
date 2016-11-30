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

import java.nio.file.{Files, Paths}

import language.higherKinds
import cats._
import cats.implicits._
import jawn.ast
import jawn.AsyncParser
import jawn.ParseException
import jawn.ast.JValue
import org.gdget._
import org.gdget.data.UNeighbourhood
import org.gdget.loom.experimental.Parsable
import org.gdget.std.all._

import scala.annotation.tailrec
import scala.io.Source

/** Collection of methods for reading graphs from our, json encoded, adjacency list format. Can either produce a stream
  * of arbitrary objects, or a stream of vertex neighbourhoods, given different parsing functions.
  *
  */
object GraphReader {

  //Type aliases for additional clarity
  type Label = String
  type ParseError = String

  /** Method reads .json files which comply with our graph format.
    *
    * Specifically, we expect graphs to be encoded as a single top-level json array, each entry of which corresponds to
    * a vertex and its edges. For example:
    *
    * {{{
    *   [
    *     {id:NodeId, label:Label, in:[{label:InEdgeLabel,id:NodeId}, {label, id}, ...], out: [{label, id}, ...]},
    *     {id:NodeId, label:Label, in:[{label:InEdgeLabel,id:NodeId}, {label, id}, ...], out: [{label, id}, ...]},
    *     ...
    *   ]
    * }}}
    *
    * Given a function `toInput` to parse elements of this array to a given type `B`, this method will either produce a
    * iterator of `B` objects, or a `ParseException` in the event that an element is malformed or a label value unrecognised.
    */
  def readLines[B](path: String, toInput: JValue => Either[String, B]): Either[ParseException, Iterator[B]] = {
    //Load the file lazily given path
    val input = Paths.get(path)
    val p = ast.JParser.async(mode = AsyncParser.UnwrapArray)
    val vertexStream: Iterator[String] = Source.fromFile(input.toFile).getLines()

    @tailrec
    def go(js: Iterator[String], inputStream: Iterator[B], line: Int): Either[ParseException, Iterator[B]] = {
      if (js.hasNext)
        p.absorb(js.next()) match {
          case Right(entries) =>
            //Note - may seem redundant to turn into a stream then immediately evaluate with traverse, but entries is a Seq
            // So its already evaluated
            entries.toList.traverse(toInput) match {
              case Right(bList) =>
                go(js, inputStream ++ bList.iterator, line + 1)
              case Left(errorMsg) =>
                Left(ParseException(errorMsg, -1, line, -1))
            }
          case Left(e) =>
            Left(e)
        }
      else
        p.finish().flatMap { entries =>
          entries.toList.traverse(toInput) match {
            case Right(bList) =>
              Right(inputStream ++ bList.iterator)
            case Left(errorMsg) =>
              Left(ParseException(errorMsg, -1, line, -1))
          }
        }
    }

    go(vertexStream, Iterator.empty, 0)
  }

  /** Method for parsing a sing Json value (`JValue`)
    *
    * Produces a `UNeighbourhood` object for a vertex from a given Vertex ADT `V`
    */
  private def jsonToNeighbourhood[V](jValue: JValue,
                                                  eToV: (Label, Int, Direction) => Either[ParseError, V],
                                                  vToV: (Label, Int) => Either[ParseError, V]): Either[String, UNeighbourhood[V, HPair]] = {

    def getId(j: JValue) = {
      val idOpt = j.get("id").getLong
      idOpt.fold(Either.left[String, Long]("Unable to properly parse vertex id"))(Either.right[String, Long])
    }

    def getCenter(j: JValue, id: Int) = {
      val lblOpt = j.get("label").getString
      lblOpt.fold(Either.left[String, V]("Unable to properly parse vertex label"))(vToV(_:String, id))
    }

    def getNeighbour(j: JValue, d: Direction) = {
      val neighbourId = getId(j)
      neighbourId.flatMap {id =>
        val lblOpt = j.get("rel").getString
        lblOpt.fold(Either.left[String, V]("Unable to properly parse edge label"))(eToV(_:String, id.toInt, d))
      }
    }

    def getNeighbours(j: JValue, d: Direction) = {
      Stream.from(0).map(j.get).takeWhile(_.nonNull).map(getNeighbour(_, d)).sequence.map(_.toList)
    }


    //TODO: Why are we using Set of unit rather than empty Set here?
    for {
      id <- getId(jValue)
      center <- getCenter(jValue, id.toInt)
      inN <- getNeighbours(jValue.get("in"), Out)
      outN <- getNeighbours(jValue.get("out"), In)
      edges = Set(())
    } yield UNeighbourhood[V, HPair](center, inN.map(_ -> edges).toMap, outN.map(_ -> edges).toMap)

  }

  /** Method to produce a stream of Neighbourhoods for vertices of type `V` provided that `V` is `Parsable`
    *
    * TODO: Modify to return `Either[ParseException, Stream[UNeighbourhood[V, HPair]]` like other methods.
    */
  def readNeighbourhoods[V: Parsable](path: String): Either[ParseException, Iterator[UNeighbourhood[V, HPair]]] = {
    readLines(path, jsonToNeighbourhood[V](_: JValue, Parsable[V].fromEdgeRepr, Parsable[V].fromRepr))
  }
}
