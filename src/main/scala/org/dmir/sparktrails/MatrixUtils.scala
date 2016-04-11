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
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MatrixUtils {
  
  def saveRowMatrix(rowMatrix: RDD[(Long, Seq[(Long, Double)])], path: String) {
    rowMatrix.
      filter(!_._2.isEmpty).
      map {
        case (row, cols) => {
          
          val colString = cols.map {
            case (col, value) => s"$col;$value"
          }.mkString(",")
          
          s"$row\t$colString"
        }
      }.
      saveAsTextFile(path)
  }
  
  def loadRowMatrix(path: String, partions: Int = 1024)(implicit sc: SparkContext): RDD[(Long, Seq[(Long, Double)])] = {
    sc.textFile(path, partions).
      map {
        line => {
          val rowSplit = line.split("\t")
          val row = rowSplit(0).toLong
          val cols = rowSplit(1).split(",").
            map { 
              colString => {
                val valueSplit = colString.split(";")
                val col = valueSplit(0).toLong
                val value = valueSplit(1).toDouble
                (col, value)
              }
            }
          (row, cols)
        }
      }
  }
  
}