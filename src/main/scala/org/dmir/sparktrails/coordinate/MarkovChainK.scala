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
package org.dmir.sparktrails.coordinate

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.commons.math3.special.Gamma

object MarkovChainK {
  
  def evidence(
      numberOfStates: Int,
      numberOfKs: Int,
      ns: RDD[((Long, Long), Double)],
      alphas: RDD[((Long, Long), Seq[Double])],
      alphaSmoothing: Double = 1)(
          implicit sc: SparkContext) = {
    
    ns.fullOuterJoin(alphas).
    map {
      case ((row, column), (optionN, optionAlpha)) => {
        
        val n = optionN.getOrElse(0.0);
        val alpha = optionAlpha.getOrElse(Array[Double](numberOfKs).toSeq);
        val stats = alpha.map { 
          a=> {
            val smoothedAlpha = a + alphaSmoothing
            val gammaAlpha = Gamma.logGamma(smoothedAlpha)
            val gammaAlphaN = Gamma.logGamma(n + smoothedAlpha)
            (n, a, gammaAlphaN - gammaAlpha)
          }
        }
        (row, stats)
      }
    }.
    reduceByKey {
      case (stats1, stats2) => {
        stats1.zip(stats2).map {
          case ((n1, alpha1, lg1), (n2, alpha2, lg2)) => {
            (n1 + n2, alpha1 + alpha2, lg1 + lg2)
          }
        }
      }
    }.
    map {
      case (row, stats) => {
        stats.map {
          case (n, alpha, lg) => {
            val smoothedAlpha = alpha + alphaSmoothing * numberOfStates
            Gamma.logGamma(smoothedAlpha) + lg - Gamma.logGamma(n + smoothedAlpha)
          }
        }
      }
    }.
    reduce {
      case (ke1, ke2) => {
        ke1.zip(ke2).map { case (e1, e2) => e1 + e2 }
      }
    }
    
  }
}