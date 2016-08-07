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
import scala.collection.mutable
import org.gdget.{Edge, Graph}

import cats._

import cats.syntax.all._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

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
sealed trait TPSTry[G[_, _[_]], V, E[_]] { self =>

  import TPSTry._

  /** Does the TPSTry contain any Nodes? */
  def isEmpty: Boolean

  /** The signature associated with this trie node's graph G. It is the product of all the multiplication factors associated with
    * branches in the TPSTry which lead to this node.
    *
    * To understand how we compute signatures for the graphs in the TPSTry, see (http://www.vldb.org/pvldb/vol8/p413-ge.pdf)
    */
  def signature: BigInt

  /** Internal representation of this TPSTry node's associated graph G. Is a set of edges */
  protected def repr: Set[E[V]]

  /** The map from Keys to child subTries
    *
    * Keys are essentially multiplication factors. A trie node's `signature`
    */
  def children: Map[Factor, TPSTry[G, V, E]]

  /** Add a child to this TPSTry node */
  protected def addChild(factor: Factor, child: TPSTry[G, V, E]): TPSTry[G, V, E] =
    Node(self.repr, self.support, self.signature, self.children + (factor -> child))

  /** Return the graph associated with this TPSTry node, if it exists. This creates a default graph instance for the type
    * G from an internal representation of a set of edges
    */
  def graphOption(implicit gEv: Graph[G], eEv: Edge[E]): Option[G[V, E]] =
    repr.headOption.map(Graph[G].point(_)).map(g => Graph[G].plusEdges(g, repr.tail.toList:_*))

  /** A simple counter of how many times the graph represented by this node has been added to the TPSTry */
  def support: Int

  /** Simple tail recursive function to check if a node exists in the trie which may be reached by traversing the given
    * sequence of factors. If the node does exist, return it and the subTPSTry for which it is the root.
    */
  @tailrec
  final def withFactors(path: Seq[Factor]): Option[TPSTry[G, V, E]] = path match {
    case head +: tail =>
      children.get(head) match {
        case None => None
        case Some(node) => node.withFactors(tail)
      }
    case _ => Some(this)
  }


  /** Add a graph to the TPSTry. Note to self: `this.type` return types are **instance** specific, so only really make
    * sense in mutable collections.
    *
    * The add function implements the "weave" algorithm described by me in (http://ceur-ws.org/Vol-1558/paper26.pdf).
    */
  def add(graph: G[V, E])(implicit gEv: Graph[G], eEv: Edge[E], sEv: Signature[V]) = {

    def corecurse(parent: TPSTry[G, V, E], es: Set[E[V]], depth: Int, sigs: SigTable[G, V, E]): TPSTry[G, V, E] = {

      //Get set of vertices from set of neighbours
      val vs = es.map(Edge[E].left) ++ es.map(Edge[E].right)
      //Get the set of all edges incident to es, including es
      val neighbourhoods = vs.flatMap(Graph[G].neighbourhood(graph, _))
      //Remove es to get the set of edges which are incident to es but not *in* es
      val ns = neighbourhoods.flatMap(_.edges) &~ es
      //Fold Left over each indicent edge en
      ns.foldLeft(parent) { (p, en) =>
        //Initialise sigsTable at this depth if it doesn't exist yet
        if(sigs.size < depth) sigs(depth) = mutable.HashMap.empty[BigInt, NodeBuilder[G, V, E]]
        // Calculate factor of en
        val (l, r) = Edge[E].vertices(en)
        val factor = Signature[V].signature(l) - Signature[V].signature(r)
        val enSig = factor * p.signature
        //create a builder if one is not found in sigTable
        val b = (sigs(depth).get(enSig), p.children.get(factor)) match {
          case (Some(existing), _) => existing.support += 1; existing
          case (None, Some(c)) => NodeBuilder(c.repr, c.support + 1, c.signature, c.children)
          case _ => NodeBuilder(es + en, 1, enSig, Map.empty[Factor, TPSTry[G, V, E]])
        }

        //Add/Update builder in map
        sigs(depth).update(enSig, b)
        //Add as child of p, and recurse, passing on mutable sigTable state
        p.addChild(factor, corecurse(b, es + en, depth + 1, sigs))
      }
    }

    //TODO: Get rid of the idea that TPSTry as a datastructure is subdivisible. It isn't really. Doesn't make sense that it should be.
    //This would mean redefining TPSTry as a case class and creating TPSTryNode sealed trait with all the same cases it has now.
    //All algorithms can still be defined recursively but can't add a graph to a Node, only to the TPSTry itself, which will keep a
    // root reference. Still need to work on a remove method, as that *does* make sense in this context. Same approach:
    //Recurse, remove support, if support is 0 remove node and stop recursing. Any time we change support, (or remove a node?), we replace with a builder
    // and put the builder in a sigTable which we carry along with us.

    val edges = Graph[G].edges(graph)
    val sigTable = mutable.ArrayBuffer.empty[mutable.HashMap[BigInt, NodeBuilder[G, V, E]]]
    edges.foldLeft(this) { (trie, edge) =>
      corecurse(trie, Set(edge), 0, sigTable)
    }

    //build() the builder Trie
  }


}

object TPSTry {

  /** Type Alias for Int: Factor to avoid confusion */
  type Factor = Int

  /** Type Alias for signature table, which is a little verbose */
  type SigTable[G[_, _[_]], V, E[_]] = mutable.ArrayBuffer[mutable.HashMap[BigInt, NodeBuilder[G, V, E]]]

  /** The empty TPSTry, essentially a root node with no children */
  def empty[G[_, _[_]], V, E[_]] = Root[G, V, E]()

  /** apply method for building TPSTry++ from provided graph */
  //TODO: Be more DRY using pattern matching.
  def apply[G[_, _[_]]: Graph, V: Signature, E[_]: Edge](g: G[V, E]): TPSTry[G, V, E] = {

    //TODO: Replace mess below with Root[...]().add(g)
    //Corecursive method to build up TPSTry from g beyond depth 1 from root
    def corecurse(n: NodeBuilder[G, V, E], g: G[V, E], es: Set[E[V]], depth: Int, sigs: SigTable[G, V, E]): TPSTry[G, V, E] = {
      //Get set of vertices from set of neighbours
      val vs = es.map(Edge[E].left) ++ es.map(Edge[E].right)
      //Get the set of all edges incident to es, including es
      val neighbourhoods = vs.flatMap(Graph[G].neighbourhood(g, _))
      //Remove es to get the set of edges which are incident to es but not *in* es
      val ns = neighbourhoods.flatMap(_.edges) &~ es
      (ns.foldLeft((n, sigs)) { (trie, e) =>
        val (parent, s) = trie
        //Initialise sigs table at this depth if it doesn't already exist
        if(s.size < depth) s(depth) = mutable.HashMap.empty[BigInt, NodeBuilder[G, V, E]]
        // Calculate factor of e
        val (l, r) = Edge[E].vertices(e)
        val factor = Signature[V].signature(l) - Signature[V].signature(r)
        //Does parent node signature * factor exist at this level of s?
        val nSig = parent.signature * factor
        val n = s(depth).get(nSig) match {
          case Some(t) =>
            //If yes then increment support
            t.support += 1
            t
          case None =>
            //Otherwise, crate a graph from es and a TPSTry node
            NodeBuilder[G, V, E](es + e, 1, nSig, Map.empty[Factor, TPSTry[G, V, E]])
        }

        //Add/Update Node n in ArrayBuffer at depth
        s(depth).update(nSig, n)

        //Recurse
        parent.children += (factor -> corecurse(n, g, es + e, depth + 1, s))
        //Return parent and sig table
        (parent, s)
      })._1
    }

    // Create empty root node (graph: None, signature: 1) Note we do *not* use empty[G, V, E] here as this returns an
    // immutable TPSTry, whilst we are trying to build one
    val r = NodeBuilder(Set.empty[E[V]], 0, 1, Map.empty[Factor, TPSTry[G, V, E]])
    // Get edges in G, es
    val es = Graph[G].edges(g)
    // Fold left over es, starting with the empty TPSTry we just created (the root node) and the an empty
    // ArrayBuffer[Map[BigInt, TPSTry]] which associates signatures to unique TPSTry nodes at a given depth.
    (es.foldLeft((r, mutable.ArrayBuffer.empty[mutable.HashMap[BigInt, NodeBuilder[G, V, E]]])) { (trie, e) =>
      val (root, sigs) = trie
      // For each e
      // Initialise a depth idx of 0
      val depth = 0
      if(sigs.size < depth) sigs(depth) = mutable.HashMap.empty[BigInt, NodeBuilder[G, V, E]]
      // Calculate factor of e
      val (l, r) = Edge[E].vertices(e)
      val factor = Signature[V].signature(l) - Signature[V].signature(r)
      // Does parent sig (1 in this case) * factor of e exist in Map[BigInt, TPSTry] at this idx in ArrayBuffer?
      val n = sigs(depth).get(factor) match {
        case Some(t) =>
          // If yes, increment support value (inside the recursive function we would also add a branch from parent node
          // to the node from the Map, but at the root level that branch is guaranteed to already exist)
          t.support += 1
          t
        case None =>
          // Else, create TPSTry node n for e
          NodeBuilder[G, V, E](Set(e), 1, factor, Map.empty[Factor, TPSTry[G, V, E]])
      }

      //Add/Update Node n in ArrayBuffer at depth
      sigs(depth).update(factor, n)

      (root.copy(support = root.support+1, children = root.children.updated(factor, corecurse(n, g, Set(e), depth+1, sigs))), sigs)
    })._1

  }

  private[loom] case class Node[G[_, _[_]], V, E[_]](repr: Set[E[V]],
                                                     support: Int,
                                                     signature: BigInt,
                                                     children: Map[Factor, TPSTry[G, V, E]]) extends TPSTry[G, V, E] {

    val isEmpty = false

  }

  private[loom] case class Tip[G[_, _[_]], V, E[_]](repr: Set[E[V]], support: Int,
                                                    signature: BigInt) extends TPSTry[G, V, E] {

    val isEmpty = true

    val children = Map.empty[Factor, TPSTry[G, V, E]]
  }

  private[loom] case class NodeBuilder[G[_, _[_]], V, E[_]](repr: Set[E[V]],
                                                            var support: Int,
                                                            signature: BigInt,
                                                            var children: Map[Factor, TPSTry[G, V, E]])
    extends TPSTry[G, V, E] {

    val isEmpty = false

    override def addChild(factor: Factor, child: TPSTry[G, V, E]): TPSTry[G, V, E] = {
      this.children += (factor -> child)
      this
    }
  }


  private[loom] case object Root extends TPSTry[Nothing, Nothing, Nothing] {

    def apply[G[_, _[_]], V, E[_]](): TPSTry[G, V, E] = this.asInstanceOf[TPSTry[G, V, E]]

    def unapply[G[_, _[_]], V, E[_]](a: TPSTry[G, V, E]) = a.signature == this.signature

    val isEmpty: Boolean = true

    val signature: BigInt = 1

    val children = Map.empty[Factor, TPSTry[Nothing, Nothing, Nothing]]

    val repr = Set.empty[Nothing]

    val support = 0

  }


}

sealed trait TPSTryInstances {}
