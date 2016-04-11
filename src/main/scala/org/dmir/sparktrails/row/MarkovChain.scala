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

object MarkovChain {
  
  def evidence(
      numberOfStates: Int,
      ns: RDD[(Long, Seq[(Long, Double)])], // (src, (dst, count))
      alphas: RDD[(Long, Map[Long, Double])], // (src, (dst, kValues))
      alphaSmoothing: Double = 1d)(
          implicit sc: SparkContext): 
              Double = {
      
    // join counts and prior
    ns.leftOuterJoin(alphas).
    map {
      case (state, (ns, alphasOption)) => {
        val alphas = alphasOption.getOrElse(Map[Long, Double]())
        Multinomial.calculateEvidence(numberOfStates, ns, alphas, alphaSmoothing)
      }
    }.
    sum
  }
}