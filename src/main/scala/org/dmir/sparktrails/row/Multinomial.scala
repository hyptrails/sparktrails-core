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

import org.apache.commons.math3.special.Gamma

object Multinomial {

  def calculateEvidence(
      numberOfStates: Int,
      ns: Seq[(Long, Double)],
      alphas: Map[Long, Double],
      alphaSmoothing: Double = 1d) = {
    
    val sumOfNs = 
      ns.map { case(dst, n) => n }.sum
      
    val sumOfAlphas = 
      alphas.map { case(dst, alpha) => alpha }.sum +
      numberOfStates * alphaSmoothing
      
//    val (sumOfLogGammaAlphas, sumOfLogGammaNAlphas) = ns.foldLeft(0d, 0d) { 
//        case ((sumOfLogGammaAlphas,  sumOfLogGammaNAlphas),(dst, n)) => {
//          
//          val smoothedAlpha = alphas.getOrElse(dst, 0d) + alphaSmoothing
//          
//          val smoothedAlphaN = smoothedAlpha + n
//          
//          val newSumOfLogGammaAlphas = 
//              sumOfLogGammaAlphas + Gamma.logGamma(smoothedAlpha)
//              
//          val newSumOfLogGammaNAlphas = 
//              sumOfLogGammaNAlphas + Gamma.logGamma(smoothedAlphaN)
//              
//          (newSumOfLogGammaAlphas, newSumOfLogGammaNAlphas)
//        }
//    }
//      
//    Gamma.logGamma(sumOfAlphas) + 
//    sumOfLogGammaNAlphas - 
//    sumOfLogGammaAlphas - 
//    Gamma.logGamma(sumOfNs + sumOfAlphas)
    
    // this version might be a little more numerically stable
    val lg = ns.foldLeft(0d) { 
      case (lg, (dst, n)) => {
        val smoothedAlpha = alphas.getOrElse(dst, 0d) + alphaSmoothing
        val smoothedAlphaN = smoothedAlpha + n
        lg + Gamma.logGamma(smoothedAlphaN) - Gamma.logGamma(smoothedAlpha)
      }
    }
      
    Gamma.logGamma(sumOfAlphas) + lg - Gamma.logGamma(sumOfNs + sumOfAlphas)
  }
  
}