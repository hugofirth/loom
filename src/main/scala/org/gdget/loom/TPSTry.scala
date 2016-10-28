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
import cats.implicits._
import org.gdget.data.SimpleGraph

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
final class TPSTry[V, E[_], P <: Field] private (val root: TPSTryNode[V, E, P], val total: Int, val p: P) {

  import TPSTryNode._

  /** Public interface to TPSTryNode's add method. Adds all subgraphs of the provided graph as Nodes in the TPSTry */
  def add[G[_, _[_]]](graph: G[V, E])(implicit gEv: Graph[G, V, E], eEv: Edge[E], vEv: Labelled[V]) = new TPSTry(root.add(graph, p), total + 1, p)

  /** TODO: Get TPSTry Node which has signature x and return it. */

  /** Simple method recurses through the TPSTry nodes and returns the sub-DAG where all nodes have a support ratio
    * greater than the provided threshold value. We call these nodes Motifs.
    */
  def motifsFor(threshold: Double): TPSTry[V, E, P] = {
    def pruneLowSupport(node: TPSTryNode[V, E, P]): TPSTryNode[V, E, P] = {
      val motifC = node.children.filter { case (_, c) => c.support.toDouble/total >= threshold }
      //TODO: Work out how to properly handle Root case, is a bit redundant at the moment
      //TODO: Tail recursive?
      if(motifC.isEmpty)
        Tip(node.repr, node.support, node.signature)
      else
        Node(node.repr, node.support, node.signature, motifC.mapValues(pruneLowSupport))
    }

    new TPSTry(pruneLowSupport(root), total, p)
  }

}

object TPSTry {

  def apply[G[_, _[_]], V, E[_], P <: Field](graph: G[V, E], p: P)(implicit gEv: Graph[G, V, E],
                                                 vEv: Labelled[V],
                                                 eEv: Edge[E]) = new TPSTry[V, E, P](TPSTryNode(graph, p), 1, p)

  def empty[V: Labelled, E[_]: Edge, P <: Field](p: P) = new TPSTry[V, E, P](TPSTryNode.empty[V, E, P], 0, p)
}


/** Internal representation of TPSTry, a recursively defined graph of TPSTryNode objects. External API does not expose
  * any way to add to a TPSTryNode graph directly, because the semantics of a TPSTry forbid sub-graphs from being valid
  * TPSTries unless they have a root node, and every sub-graph of each graph represented by a node.
  *
  * TODO: Finish adding mod prime p to all signature based operations
  *
  * @tparam V
  * @tparam E
  */
sealed trait TPSTryNode[V, E[_], P <: Field] { self =>

  import TPSTryNode._

  /** Does the TPSTry contain any Nodes? */
  def isEmpty: Boolean

  /** The signature associated with this trie node's graph G. It is the product of all the multiplication factors associated with
    * branches in the TPSTry which lead to this node.
    *
    * To understand how we compute signatures for the graphs in the TPSTry, see (http://www.vldb.org/pvldb/vol8/p413-ge.pdf)
    */
  def signature: Signature[P]

  /** Internal representation of this TPSTry node's associated graph. Is a set of edges */
  protected[loom] def repr: Set[E[V]]

  /** The map from Keys to child subTries
    *
    * Keys are essentially multiplication factors. A trie node's `signature`
    */
  def children: Map[Signature[P], TPSTryNode[V, E, P]]

  /** Add a child to this TPSTry node */
  protected def addChild(factor: Signature[P], child: TPSTryNode[V, E, P]): TPSTryNode[V, E, P] =
    Node(self.repr, self.support, self.signature, self.children + (factor -> child))

  /** Return the graph associated with this TPSTry node, if it exists. This creates a default graph instance for the type
    * G from an internal representation of a set of edges
    */
  def graphOption[G[_, _[_]]](implicit gEv: Graph[G, V, E], eEv: Edge[E]): Option[G[V, E]] =
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
  final def withFactors(path: Seq[Signature[P]]): Option[TPSTryNode[V, E, P]] = path match {
    case head +: tail =>
      children.get(head) match {
        case None => None
        case Some(node) => node.withFactors(tail)
      }
    case _ => Some(self)
  }


  /** Add a graph to the TPSTry. Note to self: `this.type` return types are **instance** specific, so only really make
    * sense in mutable collections.
    *
    * The add function implements the "weave" algorithm described by me in (http://ceur-ws.org/Vol-1558/paper26.pdf).
    */
  private[loom] def add[G[_, _[_]]](graph: G[V, E], prime: P)(implicit gEv: Graph[G, V, E], eEv: Edge[E], sEv: Labelled[V]) = {

    def corecurse(parent: TPSTryNode[V, E, P], es: Set[E[V]], depth: Int, sigs: SigTable[V, E, P]): TPSTryNode[V, E, P] = {

      //Get set of vertices from set of edges, or initial graph
      val vs = if (es.isEmpty)
        Graph[G, V, E].vertices(graph).toSet
      else
        es.flatMap { e =>
          val (l, r) = Edge[E].vertices(e)
          l :: r :: Nil
        }

      //Get the set of all edges incident to es, including es
      val neighbourhoods = vs.flatMap(Graph[G, V, E].neighbourhood(graph, _))
      //Remove es to get the set of edges which are incident to es but not *in* es
      val ns = neighbourhoods.flatMap(_.edges) &~ es
      //Fold Left over each incident edge en
      ns.foldLeft(parent) { (p, en) =>
        //Initialise sigsTable at this depth if it doesn't exist yet
        if(sigs.size <= depth) sigs += mutable.HashMap.empty[Signature[P], NodeBuilder[V, E, P]]

        val factor = Signature.forAdditionToEdges(en, es, prime)
        // Calculate factor of en
        val (l, r) = Edge[E].vertices(en)
//        //Create simplegraph from parent repr
//        val pG = SimpleGraph((es + en).toSeq:_*)
//        //Get neighbourhoods for both l & r in pG and find out their current degree, or else its 0
//        val lD = Graph[SimpleGraph, V, E].neighbourhood(pG, l).map(_.neighbours.size).getOrElse(0)
//        val rD = Graph[SimpleGraph, V, E].neighbourhood(pG, r).map(_.neighbours.size).getOrElse(0)
//        //Calculate the new degree factor for both l & r
//        val rDegFactor = ((Labelled[V].label(r) + (rD + 1)).abs % prime) + 1
//        val lDegFactor = ((Labelled[V].label(l) + (lD + 1)).abs % prime) + 1
//        val eFactor = ((Labelled[V].label(l) - Labelled[V].label(r)).abs % prime) + 1
//        //Calculate combined factor and new signature
//        val factor = rDegFactor * lDegFactor * eFactor
        val enSig = p.signature |+| factor
        //create a builder if one is not found in sigTable with same signature and size
        val b = (sigs(depth).get(enSig), p.children.get(factor)) match {
          case (Some(existing), _) => existing
          case (None, Some(c)) => NodeBuilder(c.repr, c.support + 1, c.signature, c.children)
          case _ => NodeBuilder(es + en, 1, enSig, Map.empty[Signature[P], TPSTryNode[V, E, P]])
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
//    val edges = Graph[G, V, E].edges(graph)
    val sigTable = mutable.ArrayBuffer.empty[mutable.HashMap[Signature[P], NodeBuilder[V, E, P]]]
    val trieBldr = corecurse(self, Set.empty[E[V]], 0, sigTable)

    //Below may have intangible benefits. Investigate.
//    edges.foldLeft(self) { (trie, edge) =>
//      //For each individual edge, add it to the trie
//      corecurse(trie, Set(edge), 0, sigTable)
//    }

    trieBldr.build()
  }


  def build(): TPSTryNode[V, E, P] = {

    def recurse(bldr: TPSTryNode[V, E, P]): TPSTryNode[V, E, P] = bldr match {
      case NodeBuilder(es, supp, sig, c) if c.isEmpty => Tip(es, supp, sig)
      case NodeBuilder(es, supp, sig, c) => Node(es, supp, sig, c.mapValues(recurse))
      case a => a
    }

    recurse(self)
  }


}

object TPSTryNode {

//  /** Type Alias for Int: Factor to avoid confusion */
//  type Factor = Int

  /** Type Alias for signature table, which is a little verbose */
  type SigTable[V, E[_], P <: Field] = mutable.ArrayBuffer[mutable.HashMap[Signature[P], NodeBuilder[V, E, P]]]

  /** The empty TPSTry, essentially a root node with no children */
  def empty[V: Labelled, E[_]: Edge, P <: Field] = Root[V, E, P]()

  /** apply method for building TPSTry++ from provided graph */
  //TODO: Verify the calling of add method on Root object below
  def apply[G[_, _[_]], V, E[_], P <: Field](g: G[V, E], p: P)(implicit gEv: Graph[G, V, E],
                                                 vEv: Labelled[V],
                                                 eEv: Edge[E]): TPSTryNode[V, E, P] = Root[V, E, P]().add(g, p)

  private[loom] case class Node[V, E[_], P <: Field](repr: Set[E[V]], support: Int, signature: Signature[P],
                                         children: Map[Signature[P], TPSTryNode[V, E, P]]) extends TPSTryNode[V, E, P] {

    val isEmpty = false

  }

  private[loom] case class Tip[G[_, _[_]], V, E[_], P <: Field](repr: Set[E[V]], support: Int,
                                                    signature: Signature[P]) extends TPSTryNode[V, E, P] {

    val isEmpty = true

    val children = Map.empty[Signature[P], TPSTryNode[V, E, P]]
  }

  //TODO: Do not make this extend TPSTryNode, mutability should be typechecked
  private[loom] case class NodeBuilder[V, E[_], P <: Field](repr: Set[E[V]], var support: Int, signature: Signature[P],
                                                var children: Map[Signature[P], TPSTryNode[V, E, P]]) extends TPSTryNode[V, E, P] {

    val isEmpty = false

    override def addChild(factor: Signature[P], child: TPSTryNode[V, E, P]): TPSTryNode[V, E, P] = {
      this.children += (factor -> child)
      this
    }
  }


  private[loom] case object Root extends TPSTryNode[Nothing, Nothing, Nothing] {

    def apply[V, E[_], P <: Field](): TPSTryNode[V, E, P] = this.asInstanceOf[TPSTryNode[V, E, P]]

    def unapply[V, E[_], P <: Field](a: TPSTryNode[V, E, P]) = this eq a

    val isEmpty: Boolean = true

    //TODO: Not really sure this is the right way to do things?
    val signature = Signature.zero[Nothing]

    val children = Map.empty[Signature[Nothing], TPSTryNode[Nothing, Nothing, Nothing]]

    val repr = Set.empty[Nothing]

    val support = 0

  }


}

sealed trait TPSTryInstances {


  /** Implement those typeclasses which make sense */
}
