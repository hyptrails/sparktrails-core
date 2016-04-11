/**
 * This file is part of SparkTrails - Core.
 *
 * SparkTrails - Core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SparkTrails - Core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SparkTrails - Core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dmir.sparktrails

import org.apache.spark.rdd.RDD
import org.dmir.sparktrails.{Matrix => BaseMatrix}
import org.dmir.sparktrails.Matrix._
import org.apache.spark.SparkContext

object HypTrails {
  
  object Hypothesis {
    
    import org.dmir.sparktrails.Matrix._
    
    type Type = Matrix.MapRow.Type
    
    def observedLinks(transitionCounts: Matrix.SeqRow.Type): Hypothesis.Type =
      transitionCounts.
      map { case (row, columnEntries) => {
        val newColumnEntries = columnEntries.map { case (id, value) => (id, 1d) }
        (row,  newColumnEntries.toMap) 
      }}.
      normalizeRows
    
  }
  
  object FlatHypothesis {
    
    type Type = Map[Long, Double]
    
    def uniform(states: Seq[Long]): FlatHypothesis.Type =  {
      val numberOfStates = states.size
      states.map(state => (state, 1d / numberOfStates)).toMap
    }
    
    def normalize(vector: Type): Type = {
      val sum = vector.map(_._2).sum
      vector.map { case (id, value) => (id, value / sum) }
    }
    
  }
  
  object Prior {
    
    type Type = RDD[(Long, Map[Long, Seq[Double]])]
    
    /**
     * Elicit priors for several concentration factors given a stochastic matrix.
     */
    def elicit(matrix: BaseMatrix.MapRow.Type, scalars: Seq[Double]): Prior.Type =
        matrix.map { 
          case (row, columnEntries) => {
            val columnEntriesK = columnEntries.map { 
              case (column, value) => (column, scalars.map { scalar => value * scalar }) }
            (row, columnEntriesK)
          }
        }
    
  }
  
  object FlatPrior {
    
    type Type = Map[Long, Seq[Double]]
    
    def elicit(flatHypothesis: FlatHypothesis.Type, ks: Seq[Double]): FlatPrior.Type = 
      flatHypothesis.map { case (id, value) => (id, ks.map( _ * value )) }
  }
  
  /**
   * Removes all source states from the hypothesis which are not observed in the transition counts.
   * This can greatly increase the runtime dependent on the sparsity of the transition counts
   * and the density of the hypothesis.
   */
  def filterPriorWithNoTransitionCountsViaBroadcast(
      transitionCounts: BaseMatrix.SeqRow.Type,
      hypothesis: BaseMatrix.MapRow.Type)(
          implicit sc: SparkContext): 
              BaseMatrix.MapRow.Type = {
    
    // collect possible source states
    val sources = transitionCounts.map { case (src, dstValues) => src }.distinct.collect.toSet
    val sourcesBc = sc.broadcast(sources)
    
    // remove source states from the hypothesis which do not appear in the transition counts
    hypothesis.filter { 
      case (src, dstValues) => sourcesBc.value.contains(src) 
    }
  }
  
  /**
   * Convenience method for calculating hypotheses based on state properties.
   * This method broadcasts all state properties.
   */
  def calculateHypothesisViaBroadcast[T](
      states: RDD[(Long, T)], 
      f: (T,T) => Double)(
          sc: SparkContext):
              Matrix.MapRow.Type = {
    
    val statesBc = sc.broadcast(states.collect)
    
    states.map {
      case (state1, t1) => {
        
        val values = statesBc.value.
          map { 
            case (state2, t2) => (state2, f(t1, t2))
          }.
          toMap
        
        (state1, values)
      }
    }
  }
  
    /**
   * Convenience method for calculating hypotheses based on state properties.
   * This method uses a cartesian product.
   */
  def calculateHypothesisViaCartesian[T](
      states: RDD[(Long, T)], 
      f: (T,T) => Double)(
          sc: SparkContext):
              Matrix.MapRow.Type = {
                
    states.cartesian(states).
    map {
      case ((source, sourceProperties), (destination, desintationProperties)) 
      => (source, (destination, f(sourceProperties, desintationProperties)))
    }.
    groupByKey.
    map { 
      case (source, targetList) 
      => (source, targetList.toMap) 
    }
  }
    
   
  
}