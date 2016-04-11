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

import org.apache.commons.math3.special.Gamma
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object MarkovChain {
  
  object Mode extends Enumeration {
    type Mode = Value
    val 
      FULL_OUTER_JOIN,
      LEFT_OUTER_JOIN_DISTRIBUTE_ALPHA_AND_N_SUMS_VIA_BROADCAST,
      LEFT_OUTER_JOIN_ADD_ALPHA_AND_N_SUMS_TO_RDD = 
        Value
  }
  import Mode._
  
  /**
   * While the implementations can handle it; it probably always makes sense to 
   * filter the "alphas" so that they only contain entries corresponding to source
   * states which have been observed, i.e., which appear in the "ns" matrix.
   * To do this use for example [[HypUtils]].
   */
  def evidence(
      numberOfStates: Int, 
      ns: RDD[((Long, Long), Double)], 
      alphas: RDD[((Long, Long), Double)], 
      alphaSmoothing: Double = 1)(
          mode: Mode = FULL_OUTER_JOIN)(
            implicit sc: SparkContext) = {
    
    mode match {
      case FULL_OUTER_JOIN => 
        calculateEvidence_fullOuterJoin(numberOfStates, ns, alphas, alphaSmoothing)
      case LEFT_OUTER_JOIN_DISTRIBUTE_ALPHA_AND_N_SUMS_VIA_BROADCAST => 
        calculateEvidence_leftOuterJoin_distributeAlphaAndNSumsViaBroadCast(numberOfStates, ns, alphas, alphaSmoothing)
      case LEFT_OUTER_JOIN_ADD_ALPHA_AND_N_SUMS_TO_RDD => 
        calculateEvidence_leftOuterJoin_addAlphaAndNSumsToRdd(numberOfStates, ns, alphas, alphaSmoothing)
      case _ => 
        calculateEvidence_fullOuterJoin(numberOfStates, ns, alphas, alphaSmoothing)
    }
  }
  
  private def calculateEvidence_leftOuterJoin_addAlphaAndNSumsToRdd(
      numberOfStates: Int, 
      ns: RDD[((Long, Long), Double)], 
      alphas: RDD[((Long, Long), Double)], 
      alphaSmoothing: Double = 1)(
            implicit sc: SparkContext) = {
      
    val nSums = ns.
      map { case ((src, dst), value) => (src, value) }.
      reduceByKey(_ + _).
      map { case (src, sum) => (src, (sum, 0d, 0d)) }
      
    val alphaSums = alphas.
      map { case ((src, dst), value) => (src, value) }.
      reduceByKey(_ + _).
      map { case (src, sum) => (src, (0d, sum, 0d)) }
    
    ns.leftOuterJoin(alphas). 
    map {
      case ((row, column), (n, optionAlpha)) => { 
        val alpha = optionAlpha.getOrElse(0.0);
        val gammaNAlpha = Gamma.logGamma(n + alpha + alphaSmoothing)
        val gammaAlpha = Gamma.logGamma(alpha + alphaSmoothing)
        (row, (0d, 0d, gammaNAlpha - gammaAlpha))
      }
    }.
    ++(nSums).
    ++(alphaSums).
    reduceByKey { 
      case ((n1, alpha1, lg1), (n2, alpha2, lg2)) => {
        (n1 + n2, alpha1 + alpha2, lg1 + lg2)
      }
    }.
    map {
      case (row, (nSum, alphaSum, lgSum)) => {
        val smoothedAlphaSum = alphaSum + alphaSmoothing * numberOfStates
        Gamma.logGamma(smoothedAlphaSum) + lgSum - Gamma.logGamma(nSum + smoothedAlphaSum)
      }
    }.
    sum
  }
  
  private def calculateEvidence_leftOuterJoin_distributeAlphaAndNSumsViaBroadCast(
      numberOfStates: Int, 
      ns: RDD[((Long, Long), Double)], 
      alphas: RDD[((Long, Long), Double)], 
      alphaSmoothing: Double = 1)(
            implicit sc: SparkContext) = {
      
    val nSums = ns.map { case ((src, dst), value) => (src, value) }.reduceByKey(_ + _).collect.toMap
    val nSumsBc = sc.broadcast(nSums)
    
    val alphaSums = alphas.
      map { case ((src, dst), value) => (src, value) }.
      reduceByKey(_ + _).
      collect.
      toMap
    val alphaSumsBc = sc.broadcast(alphaSums)
    
    ns.leftOuterJoin(alphas).map {
      case ((row, column), (n, alphaOption)) => {
        val alpha = alphaOption.getOrElse(0d)
        val gammaNAlpha = Gamma.logGamma(n + alpha + alphaSmoothing)
        val gammaAlpha = Gamma.logGamma(alpha + alphaSmoothing)
        (row, gammaNAlpha - gammaAlpha)
      }
    }.
    reduceByKey(_ + _).
    map {
      case (row, lgSum) => {
        
        val nSum = nSumsBc.value.getOrElse(row, 0d)
        val alphaSum = alphaSumsBc.value.getOrElse(row, 0d)
        
        println(s"blah $row => $nSum, $alphaSum, $lgSum")
        
        val smoothedAlphaSum = alphaSum + alphaSmoothing * numberOfStates
        Gamma.logGamma(smoothedAlphaSum) + lgSum - Gamma.logGamma(nSum + smoothedAlphaSum)
      }
    }.
    sum
  }

  private def calculateEvidence_fullOuterJoin(
      numberOfStates: Int, 
      ns: RDD[((Long, Long), Double)], 
      alphas: RDD[((Long, Long), Double)], 
      alphaSmoothing: Double = 1)(
          implicit sc: SparkContext) = {
    
    ns.fullOuterJoin(alphas).
    map {
      case ((row, column), (optionN, optionAlpha)) => {
        
        val n = optionN.getOrElse(0.0);
        val alpha = optionAlpha.getOrElse(0.0);
        
        val gammaNAlpha = Gamma.logGamma(n + alpha + alphaSmoothing)
        val gammaAlpha = Gamma.logGamma(alpha + alphaSmoothing)
        
        (row, (n, alpha, gammaNAlpha - gammaAlpha))
      }
    }.
    reduceByKey { 
      case ((n1, alpha1, lg1), (n2, alpha2, lg2)) => {
        (n1 + n2, alpha1 + alpha2, lg1 + lg2)
      }
    }.
    map {
      case (row, (nSum, alphaSum, lgSum)) => {
        val smoothedAlphaSum = alphaSum + alphaSmoothing * numberOfStates
        Gamma.logGamma(smoothedAlphaSum) + lgSum - Gamma.logGamma(nSum + smoothedAlphaSum)
      }
    }.
    sum
    
  }
  
}