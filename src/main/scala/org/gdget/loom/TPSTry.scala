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

import language.higherKinds
import org.gdget.{Edge, Graph}

/** A trie like datastructure which captures common patterns of edge traversals encoded in a workload of graph pattern
  * matching queries.
  *
  * It is called the (T)raversal (P)attern (S)ummary Trie (Try).
  *
  * The datastructure is originally proposed here (https://arxiv.org/abs/1603.04626). This implementation represents an
  * extension from a Trie, to a directed acyclic graph, in order to capture more complex patterns of traversals in
  * queries. It is discussed here (http://ceur-ws.org/Vol-1558/paper26.pdf).
  *
  * @author hugofirth
  */
sealed trait TPSTry[G[_, _[_]], V, E[_]] {

  /** Does the TPSTry contain any Nodes? */
  def isEmpty: Boolean

  /** The key associated with this trie node.
    *
    * NOTE: This key should be unique amongst its siblings.
    */
  def key: Int

  /** The map from Keys to child subTries */
  def children: Map[Int, TPSTry[G, V, E]]

  /** Return the graph associated with this TPSTry node, if it exists */
  def rootOption: Option[G[V, E]]

  /** A simple counter of how many times the graph represented by this node has been added to the TPSTry */
  def support: Int

  /** Add a graph to the TPSTry. Note to self: `this.type` return types are **instance** specific, so only really make
    * sense in mutable collections.
    *
    * The add function implements the "Number Theoretic signatures" algorithm described by Song et al
    * (http://www.vldb.org/pvldb/vol8/p413-ge.pdf), storing separate multiplication factors for each node.
    */
  def add(graph: G[V, E])(implicit gEv: Graph[G], eEv: Edge[E], sEv: Signature[V]) = {
    def weave(e: E[V]): TPSTry[G, V, E] = {
      //Calculate Edge factor - note that we are assuming unlabelled edges
      val (l, r) = Edge[E].vertices(e)
      val eSig = Signature[V].signature(l) - Signature[V].signature(r)
      //Does the TPSTry contain this signature?
      //If not, then
      ???
    }

    //Weave algorithm here we go
    val edges = Graph[G].edges(graph)
  }


}

object TPSTry {

  private[loom] case class Node[G[_, _[_]], V, E[_]](rootOption: G[V, E], support: Int, key: Int, children: Map[Int, TPSTry[G, V, E]])
    extends TPSTry[G, V, E] {

    val isEmpty = false

  }


}

sealed trait TPSTryInstances {}
