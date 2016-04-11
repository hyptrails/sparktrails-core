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

object MultinomialK {

  def calculateEvidence(
      numberOfStates: Int,
      numberOfKs: Int,
      ns: Seq[(Long, Double)], 
      rowPrior: Map[Long, Seq[Double]], // (dst, kAlphas)
      alphaSmoothing: Double = 1d): 
        Seq[Double] = {
    
    // calculate the sum of observed transitions for DENOM2
    val sumOfN = ns.map { case (dst, count) => count}.sum
    
    
    // calculate the sum of prior values for ENUM1 and DENUM2
    val kSumsOfAlphas = rowPrior.
      foldLeft(new Array[Double](numberOfKs)) { 
        case (kAlphaSums, (dst, kAlphas)) => {
          kAlphaSums.zip(kAlphas).map { 
            case (alphaSum, alpha) => alphaSum + alpha }
        }
      }.
      map { alpha => alpha + numberOfStates * alphaSmoothing }
      
    // calculate the sum of lgamma(alpha) for DENUM1
    // and the sum of lgamma(n + alpha) for ENUM2
    // NOTE: we only sum up (n, alpha) pairs where "n > 0" since 
    //   if it does not the corresponding summands of 
    //   ENUM2 lgamma(n + alpha) = lgamma(0 + alpha) and 
    //   DEMUN1 lgamma(alpha) will cancel each other out
    val (kSumsOfLogGammaAlphas, kSumsOfLogGammaNAlphas) =
      
      ns.foldLeft((new Array[Double](numberOfKs), new Array[Double](numberOfKs))){
      
        case ((kSumsOfLogGammaAlphas, kSumsOfLogGammaNAlphas), (dst, n)) => {
          
          val kAlphas = rowPrior.getOrElse(dst, new Array[Double](numberOfKs).toSeq)
          
          val kSumsOfLogGammaAlphasNew = kSumsOfLogGammaAlphas.zip(kAlphas).map { 
            case (sumOfLogGammaAlphas, alpha) => sumOfLogGammaAlphas + Gamma.logGamma(alpha + alphaSmoothing)
          }
          
          val kSumsOfLogGammaNAlphasNew = kSumsOfLogGammaNAlphas.zip(kAlphas).map { 
            case (sumsOfLogGammaNAlphas, alpha) => sumsOfLogGammaNAlphas + Gamma.logGamma(n + alpha + alphaSmoothing)
          }
          
          (kSumsOfLogGammaAlphasNew, kSumsOfLogGammaNAlphasNew)
        }
      }
    
    
    // calculate the actual evidence: ENUM1 + ENUM2 - DENUM1 - DENUM2
    kSumsOfAlphas.zip(kSumsOfLogGammaAlphas).zip(kSumsOfLogGammaNAlphas).map {
      case ((sumOfAlphas, sumOfLogGammaAlphas), sumOfLogGammaNAlphas) => {
        Gamma.logGamma(sumOfAlphas) + 
        sumOfLogGammaNAlphas - 
        sumOfLogGammaAlphas - 
        Gamma.logGamma(sumOfN + sumOfAlphas)
      }
    }
  }
}