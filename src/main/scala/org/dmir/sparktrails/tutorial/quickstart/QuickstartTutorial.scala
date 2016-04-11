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
package org.dmir.sparktrails.tutorial.quickstart

import org.dmir.sparktrails.row.MarkovChainK
import org.apache.spark.SparkContext
import org.dmir.sparktrails.HypTrails
import org.dmir.sparktrails.MatrixUtils
import org.dmir.sparktrails.Matrix._
import java.io.PrintWriter
import org.apache.spark.SparkConf

object QuickstartTutorial {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    run(sc)
  }
  
  def run(sc: SparkContext): Unit = {
    
    val prefix = "src/main/resources/tutorial/quickstart"
    
    // set the number of states
    val numberOfStates = 4
    
    // load the transition count matrix
    val d = MatrixUtils.loadRowMatrix(s"$prefix/transitioncounts.row")(sc)
    
    // load hypotheses
    val h1 = MatrixUtils.loadRowMatrix(s"$prefix/hypothesis1.row")(sc).toMapRows
    val h2 = MatrixUtils.loadRowMatrix(s"$prefix/hypothesis2.row")(sc).toMapRows
    
    // define the concentration parameters to use
    val ks = Seq[Double](1,2,3,4,5,10,100,1000,10000)
    
    // calculate the log of the marginal likelihood
    // for each hypothesis and each k
    val e1 = MarkovChainK.evidence(
        numberOfStates, ks.size, d, HypTrails.Prior.elicit(h1, ks))(sc)
    val e2 = MarkovChainK.evidence(
        numberOfStates, ks.size, d, HypTrails.Prior.elicit(h2, ks))(sc)
        
    // output results
    val w = new PrintWriter("out/results.csv")
    w.append(s"hypothesis,k,evidence\n")
    ks.zip(e1).foreach { case (k, e) => w.append(s"h1,$k,$e\n") }
    ks.zip(e2).foreach { case (k, e) => w.append(s"h2,$k,$e\n") }
    w.close()
  }
}