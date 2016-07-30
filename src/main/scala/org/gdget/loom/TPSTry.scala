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
sealed trait TPSTry[G[_, _[_]], V, E[_]] {

  import TPSTry._

  /** Does the TPSTry contain any Nodes? */
  def isEmpty: Boolean

  /** The signature associated with this trie node's graph G. It is the product of all the multiplication factors associated with
    * branches in the TPSTry which lead to this node.
    *
    * To understand how we compute signatures for the graphs in the TPSTry, see (http://www.vldb.org/pvldb/vol8/p413-ge.pdf)
    */
  def signature: BigInt

  /** The map from Keys to child subTries
    *
    * Keys are essentially multiplication factors. A trie node's `signature`
    */
  def children: Map[Factor, TPSTry[G, V, E]]

  /** Return the graph associated with this TPSTry node, if it exists */
  def valueOption: Option[G[V, E]]

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

    //Recursive function to build sigTable. Not Tail recursive so a risk of stack overflow.
    //TODO: Clean this up. Is there a tree merge algorithm which will mean we don't have to build this table?
    //If not - then perhaps make it tail recursive somehow.
    def sigTable(node: TPSTry[G, V, E], depth: Int,
                 table: mutable.ArrayBuffer[mutable.HashMap[BigInt, TPSTry[G, V, E]]]):
    mutable.ArrayBuffer[mutable.HashMap[BigInt, TPSTry[G, V, E]]] = {
      //Initialise table at depth
      table(depth) = mutable.HashMap.empty[BigInt, TPSTry[G, V, E]]
      //Add signature->node pairs to table for all children of node, then do the same for the children, at depth+1
      node.children.values.foldLeft(table) { (acc, child) =>
        table(depth).update(child.signature, child)
        sigTable(child, depth + 1, table)
      }
    }

    def corecurse(trie: TPSTry[G, V, E], e: E[V], fac: Seq[Factor]): TPSTry[G, V, E] = {
      ???
    }

    /** Attempt 3 */
    // //Outside recursive "loop"
    // Create empty root node (graph: None, signature: 1)
    val r = Root[G, V, E]()
    // Get edges in G, es
    val es = Graph[G].edges(graph)
    // Fold left over es, starting with the empty TPSTry we just created (the root node) and the an empty
    // ArrayBuffer[Map[BigInt, TPSTry]] which associates signatures to unique TPSTry nodes at a given depth.
    es.foldLeft((r, mutable.ArrayBuffer.empty[mutable.HashMap[BigInt, TPSTry[G, V, E]]])) { (trie, e) =>
      val (tpstry, sigs) = trie
      // For each e
      // Initialise a depth idx of 0
      val depth = 0
      if(sigs.size < depth) sigs(depth) = mutable.HashMap.empty[BigInt, TPSTry[G, V, E]]
      // Calculate factor of e
      val (l, r) = Edge[E].vertices(e)
      val factor = Signature[V].signature(l) - Signature[V].signature(r)
      // Does parent sig (1 in this case) * factor of e exist in Map[BigInt, TPSTry] at this idx in ArrayBuffer?
      val n = sigs(depth).get(factor) match {
        case Some(t) => Node(t.valueOption, t.support+1, t.signature, t.children)
        case None => Node(None /* fix this */, 1, factor, Map.empty[Factor, TPSTry[G, V, E]])
      }

      val n = sigs(depth).get(factor).fold(Node(Graph[G].point(e), 1, factor, Map.empty[Factor, TPSTry[G, V, E]])) { t =>
        Node(t.valueOption, t.support+1, t.signature, t.children)
      }

      sigs(depth).update(factor, n)

      tpstry.children.updated(factor, /* recursive call */)

      // If yes, increment support value (inside the recursive function we would also add a branch from parent node to
      // the node from the Map, but at the root level that branch is guaranteed to already exist)
      // Else, create TPSTry node n for e, also add to ArrayBuffer
      // TPSTry root.children update(factor -> recurse(n, Set[E](e), idx+1))
      // // Recursive function starts here
      // Get neighbours ns of e which aren't in the Set[E]
      // fold left over ns, taking ArrayBuffer and parent TPSTry node n
      //  Calculate the factor of ns * parent sig n.signature
      //  Check if that signature ns * parent sig exists in Map[BigInt, TPSTry] at this idx in the ArrayBuffer?
      //  If yes, increment support value and get corresponding TPSTry node n'
      //  Else Create TPSTry node n' for ns. Also add to ArrayBuffer at depth
      //  parent.children update(ns factor -> recurse(n', Set[E](e, ns), idx+1
      ???
    }




    /** Attempt 2 */
    //Construction of TPSTry from a single G
    //Add empty (None) root node, gets us TPSTry t
    //FoldLeft over edges e in G. (Carry along a Vector[Map[BigInt, TPSTry]] lookup, maybe as well as an int idx)
    //For each e,
    //  calculate its Factor,
    //  calculate sig as factor * sig of parent
    //  check lookup for that BigInt at this idx in the vector
    //  if it exists, then get the TPSTry node n and increment support
    //  else, create e's corresponding TPSTry Node n and add it to lookup at idx
    //  add n as a child of t, giving us a new TPSTry t1
    //
    //  Get the set of e's incident edges e1 which aren't in e
    //  Increment table idx, pass on the trie root and the parent node of e1.
    //  For each e+e1 (back to 108) ...

    // NOTE: Before you move on to the next e in the outermost Foreach loop, you should set idx back to 1.




    /** Attempt 1 */
    //FoldLeft over edges in graph, starting with *this* TPSTry. For each edge, call co-recurse.

      //Corecurse takes a graph, an edge, a signature, a factor, and a TPSTry ?
        //(OR it could maybe take a TPSTry, an edge and a Seq of factors)
        //Does the TPSTry node given by the signature (Seq of factors + factor) exist? //How do we do this lookup to create Dag?
          //If yes
            //Is there a branch from TPSTry(Seq of factors) with key/factor == edge factor?
              //If no - create one
            //Incremement support value of TPSTry(Seq of factors + edge factor)
          //If no then get parent graph add e, and create child node with edge factor key and new g + signature.
            //Give support value of 1.
        //Does edge have any incident neighbours not in graph ?


    //Signature Table an ArrayBuffer of Sets of BigInts
    var sigs = sigTable(this, 0, new mutable.ArrayBuffer[mutable.HashMap[BigInt, TPSTry[G, V, E]]])

    //Weave algorithm here we go
    val edges = Graph[G].edges(graph)
  }


}

object TPSTry {

  /** Type Alias for Int: Factor to avoid confusion */
  type Factor = Int

  /** The empty TPSTry, essentially a root node with no children */
  def empty[G[_, _[_]], V, E[_]] = Root[G, V, E]()

  private[loom] case class Node[G[_, _[_]], V, E[_]](valueOption: Option[G[V, E]],
                                                     support: Int,
                                                     signature: BigInt,
                                                     children: Map[Factor, TPSTry[G, V, E]]) extends TPSTry[G, V, E] {

    val isEmpty = false

  }

  private[loom] case object Root extends TPSTry[Nothing, Nothing, Nothing] {

    def apply[G[_, _[_]], V, E[_]](): TPSTry[G, V, E] = this.asInstanceOf[TPSTry[G, V, E]]

    def unapply[G[_, _[_]], V, E[_]](a: TPSTry[G, V, E]) = a.isEmpty

    val isEmpty: Boolean = true

    val signature: BigInt = 1

    val children = Map.empty[Factor, TPSTry[Nothing, Nothing, Nothing]]

    val valueOption = None

    val support = 0
  }


}

sealed trait TPSTryInstances {}
