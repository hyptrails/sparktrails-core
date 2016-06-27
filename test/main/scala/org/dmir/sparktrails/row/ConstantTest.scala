package org.dmir.sparktrails.row

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalactic.TolerantNumerics

class ConstantTest extends FlatSpec with Matchers {
  
  "MultinomialConstant" should "return the same evidence as HypTrails with all alphas set to zero" in {
    
    val numberOfStates = 5
    val ns = Seq((0l,5d),(2l,1d),(3l,5d),(5l,2d))
    
    val alphas = Map[Long, Double]()
    
    val e1 = MultinomialConstant.calculateEvidence(numberOfStates, ns)
    val e2 = Multinomial.calculateEvidence(numberOfStates, ns, alphas)
    
    e1 should equal (e2)
  }
  
}