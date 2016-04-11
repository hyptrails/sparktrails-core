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
package org.dmir.sparktrails.row

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.dmir.sparktrails.Matrix
import org.dmir.sparktrails.HypTrails

object MarkovChainK {
  
  def evidence(
      numberOfStates: Int, 
      numberOfKs: Int, 
      transitionCounts: Matrix.SeqRow.Type,  // (src, (dst, count)), 
      priorParameters: HypTrails.Prior.Type,  // (src, (dst, kValues)), 
      priorSmoothing: Double = 1d)(
          implicit sc: SparkContext): 
              Seq[Double] = {
      
    transitionCounts.leftOuterJoin(priorParameters).
    map {
      case (state, (ns, alphasOption)) => {
        val alphas = alphasOption.getOrElse(Map[Long, Seq[Double]]()) 
        MultinomialK.calculateEvidence(
            numberOfStates, 
            numberOfKs, 
            ns, 
            alphas, 
            priorSmoothing)
      }
    }.
    reduce {
      case (evidenceK1, evidenceK2) => {
        evidenceK1.zip(evidenceK2).
          map { 
            case (e1,e2) => e1 + e2 
          }
      }
    }
  }
  
  def evidenceFromFlatHypothesis(
      numberOfStates: Int, 
      numberOfKs: Int, 
      transitionCounts: Matrix.SeqRow.Type,  // (src, (dst, count))
      priorParameters: HypTrails.FlatPrior.Type,  // (src, (dst, kValues))
      priorSmoothing: Double = 1d)(
          implicit sc: SparkContext): 
              Seq[Double] = {
    
    val bc_alphas = sc.broadcast(priorParameters)
    
    transitionCounts.
    map {
      case (state, ns) => {
        MultinomialK.calculateEvidence(
            numberOfStates, 
            numberOfKs, 
            ns, 
            bc_alphas.value, 
            priorSmoothing)
      }
    }.
    reduce {
      case (evidenceK1, evidenceK2) => {
        evidenceK1.zip(evidenceK2).
          map { 
            case (e1,e2) => e1 + e2 
          }
      }
    }
  }
}