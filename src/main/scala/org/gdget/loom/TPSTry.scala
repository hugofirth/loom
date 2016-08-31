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
  * TODO: Make constructor private
  *
  * @author hugofirth
  */
final class TPSTry[G[_, _[_]], V, E[_]] private (val root: TPSTryNode[G, V, E]) {

  /** Public interface to TPSTryNode's add method. Adds all subgraphs of the provided graph as Nodes in the TPSTry */
  def add(graph: G[V, E])(implicit gEv: Graph[G, V, E], eEv: Edge[E], vEv: Labelled[V]) = root.add(graph)

  /** Get TPSTry Node which has signature x and return it. */


  /** TODO: move or mirror withFactors up here? */

  /** TODO: Make motifs method to return sub-TPSTry */

}

object TPSTry {

  def apply[G[_, _[_]], V, E[_]](graph: G[V, E])(implicit gEv: Graph[G, V, E],
                                                 vEv: Labelled[V],
                                                 eEv: Edge[E]) = new TPSTry[G, V, E](TPSTryNode(graph))

  def empty[G[_, _[_]], V, E[_]](implicit gEv: Graph[G, V, E],
                                 vEv: Labelled[V],
                                 eEv: Edge[E])  = new TPSTry[G, V, E](TPSTryNode.empty[G, V, E])
}


/** Internal representation of TPSTry, a recursively defined graph of TPSTryNode objects. External API does not expose
  * any way to add to a TPSTryNode graph directly, because the semantics of a TPSTry forbid sub-graphs from being valid
  * TPSTries unless they have a root node, and every sub-graph of each graph represented by a node.
  *
  * @tparam G
  * @tparam V
  * @tparam E
  */
sealed trait TPSTryNode[G[_, _[_]], V, E[_]] { self =>

  import TPSTryNode._

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
  def children: Map[Factor, TPSTryNode[G, V, E]]

  /** Add a child to this TPSTry node */
  protected def addChild(factor: Factor, child: TPSTryNode[G, V, E]): TPSTryNode[G, V, E] =
    Node(self.repr, self.support, self.signature, self.children + (factor -> child))

  /** Return the graph associated with this TPSTry node, if it exists. This creates a default graph instance for the type
    * G from an internal representation of a set of edges
    */
  def graphOption(implicit gEv: Graph[G, V, E], eEv: Edge[E]): Option[G[V, E]] =
    repr.headOption.map(Graph[G, V, E].point(_)).map(g => Graph[G, V, E].plusEdges(g, repr.tail.toList:_*))

  /** A simple counter of how many times the graph represented by this node has been added to the TPSTry */
  def support: Int

  /** Simple tail recursive function to check if a node exists in the trie which may be reached by traversing the given
    * sequence of factors. If the node does exist, return it and the sub-graph for which it is the root.
    *
    * Note: Pattern match rather than fold, flatMap and friends because tail recursion.
    *
    * Purpose of leaving this public is for cheap matching later, essentially allowing people to keep a reference to a
    * specific location in the TPSTry, without holding on to a BigInt and calling expensive .get all the time.
    */
  @tailrec
  final def withFactors(path: Seq[Factor]): Option[TPSTryNode[G, V, E]] = path match {
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
  private[loom] def add(graph: G[V, E])(implicit gEv: Graph[G, V, E], eEv: Edge[E], sEv: Labelled[V]) = {

    def corecurse(parent: TPSTryNode[G, V, E], es: Set[E[V]], depth: Int, sigs: SigTable[G, V, E]): TPSTryNode[G, V, E] = {

      //Get set of vertices from set of neighbours
      val vs = es.map(Edge[E].left) ++ es.map(Edge[E].right)
      //Get the set of all edges incident to es, including es
      val neighbourhoods = vs.flatMap(Graph[G, V, E].neighbourhood(graph, _))
      //Remove es to get the set of edges which are incident to es but not *in* es
      val ns = neighbourhoods.flatMap(_.edges) &~ es
      //Fold Left over each indicent edge en
      ns.foldLeft(parent) { (p, en) =>
        //Initialise sigsTable at this depth if it doesn't exist yet
        if(sigs.size < depth) sigs(depth) = mutable.HashMap.empty[BigInt, NodeBuilder[G, V, E]]
        // Calculate factor of en
        val (l, r) = Edge[E].vertices(en)
        val factor = Labelled[V].label(l) - Labelled[V].label(r)
        val enSig = factor * p.signature
        //create a builder if one is not found in sigTable
        val b = (sigs(depth).get(enSig), p.children.get(factor)) match {
          case (Some(existing), _) => existing.support += 1; existing
          case (None, Some(c)) => NodeBuilder(c.repr, c.support + 1, c.signature, c.children)
          case _ => NodeBuilder(es + en, 1, enSig, Map.empty[Factor, TPSTryNode[G, V, E]])
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


    //TODO: Fix for depth 0/1, never actually adds single edges to the trie
    val edges = Graph[G, V, E].edges(graph)
    val sigTable = mutable.ArrayBuffer.empty[mutable.HashMap[BigInt, NodeBuilder[G, V, E]]]
    edges.foldLeft(this) { (trie, edge) =>
      corecurse(trie, Set(edge), 0, sigTable)
    }

    //build() the builder Trie
  }


}

object TPSTryNode {

  /** Type Alias for Int: Factor to avoid confusion */
  type Factor = Int

  /** Type Alias for signature table, which is a little verbose */
  type SigTable[G[_, _[_]], V, E[_]] = mutable.ArrayBuffer[mutable.HashMap[BigInt, NodeBuilder[G, V, E]]]

  /** The empty TPSTry, essentially a root node with no children */
  def empty[G[_, _[_]], V, E[_]](implicit gEv: Graph[G, V, E],
                                                 vEv: Labelled[V],
                                                 eEv: Edge[E]) = Root[G, V, E]()

  /** apply method for building TPSTry++ from provided graph */
  def apply[G[_, _[_]], V, E[_]](g: G[V, E])(implicit gEv: Graph[G, V, E],
                                                 vEv: Labelled[V],
                                                 eEv: Edge[E]): TPSTryNode[G, V, E] = Root[G, V, E]().add(g)

  private[loom] case class Node[G[_, _[_]], V, E[_]](repr: Set[E[V]],
                                                     support: Int,
                                                     signature: BigInt,
                                                     children: Map[Factor, TPSTryNode[G, V, E]]) extends TPSTryNode[G, V, E] {

    val isEmpty = false

  }

  private[loom] case class Tip[G[_, _[_]], V, E[_]](repr: Set[E[V]], support: Int,
                                                    signature: BigInt) extends TPSTryNode[G, V, E] {

    val isEmpty = true

    val children = Map.empty[Factor, TPSTryNode[G, V, E]]
  }

  private[loom] case class NodeBuilder[G[_, _[_]], V, E[_]](repr: Set[E[V]],
                                                            var support: Int,
                                                            signature: BigInt,
                                                            var children: Map[Factor, TPSTryNode[G, V, E]])
    extends TPSTryNode[G, V, E] {

    val isEmpty = false

    override def addChild(factor: Factor, child: TPSTryNode[G, V, E]): TPSTryNode[G, V, E] = {
      this.children += (factor -> child)
      this
    }
  }


  private[loom] case object Root extends TPSTryNode[Nothing, Nothing, Nothing] {

    def apply[G[_, _[_]], V, E[_]](): TPSTryNode[G, V, E] = this.asInstanceOf[TPSTryNode[G, V, E]]

    def unapply[G[_, _[_]], V, E[_]](a: TPSTryNode[G, V, E]) = a.signature == this.signature

    val isEmpty: Boolean = true

    val signature: BigInt = 1

    val children = Map.empty[Factor, TPSTryNode[Nothing, Nothing, Nothing]]

    val repr = Set.empty[Nothing]

    val support = 0

  }


}

sealed trait TPSTryInstances {


  /** Implement those typeclasses which make sense */
}
