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

object Matrix {
  
  object Coordinate {
    type Type = RDD[((Long, Long), Double)]
  }
  
  object SeqRow {
    
    type Type = RDD[(Long, Seq[(Long, Double)])]
    
    def sort(matrix: Type): Type =
      matrix.map { 
        case (row, columnEntries) => (row, columnEntries.sortBy(_._1))
      }
    
    def normalizeRows(matrix: Type): Type =
      matrix.map {
        case (row, columnEntries) =>  {
          val sum = columnEntries.map(_._2).sum
          val normalizedColumnEntries = columnEntries.
              map { case (column, value) => (column, value / sum) }
          (row, normalizedColumnEntries)
        }
      }
  }
  
  object MapRow {
    
    type Type = RDD[(Long, Map[Long, Double])]
    
    def normalizeRows(matrix: Type): Type =
      matrix.map {
        case (row, columnEntries) =>  {
          val sum = columnEntries.values.sum
          val normalizedColumnEntries = columnEntries.
            map { case (column, value) => (column, value / sum) }
          (row, normalizedColumnEntries)
        }
      }
    
    def multiply(matrix: Type, scalar: Double): Type =
      matrix.map { case (row, columnEntries) => {
        
        val newColumnEntries = columnEntries.map { 
            case (column, value) => (column, value * scalar) }
        
        (row, newColumnEntries)
        
      }}
  }
  
  object ArrayRow {
    
    type Type = RDD[(Long, (Array[Long], Array[Double]))]
    
    def sort(matrix: Type): Type =
      matrix.map { 
        case (row, (columns, values)) => {
          val sorted = columns.zip(values).toSeq.sortBy(_._1)
          (row, (sorted.map(_._1).toArray, sorted.map(_._2).toArray))
        }
      }
    
    def normalizeRows(matrix: Type): Type =
      matrix.map {
        case (row, (columns, values)) =>  {
          val sum = values.sum
          val normalizedValues = values.map(_ / sum)
          (row, (columns, normalizedValues))
        }
      }
  }
  
  
  implicit class SeqRowWrapper(matrix: SeqRow.Type) {
    def sort: SeqRow.Type = SeqRow.sort(matrix)
    def normalizeRows: SeqRow.Type = SeqRow.normalizeRows(matrix)
    def toMapRows: MapRow.Type = fromSeqToMapRows(matrix)
  }
  
  implicit class MapRowWrapper(matrix: MapRow.Type) {
    def normalizeRows: MapRow.Type = MapRow.normalizeRows(matrix)
  }
  
  implicit class CoordinateWrapper(matrix: Coordinate.Type) {
    def toSeqRows = fromCoordinateToSeqRows(matrix)
  }
  

  def fromSeqToMapRows(rowCounts: SeqRow.Type): MapRow.Type =
    rowCounts.map { 
        case (source, targetCounts) 
        =>  (source, targetCounts.toMap)}
  
  def fromSeqToArrayRows(rowCounts: SeqRow.Type): ArrayRow.Type =
    rowCounts.map { 
        case (source, targetCounts) 
        => (source, (targetCounts.map(_._1).toArray, targetCounts.map(_._2).toArray))}
  
  def fromMapToArrayRows(rowCounts: MapRow.Type): ArrayRow.Type =
    rowCounts.map { 
        case (source, targetCounts) 
        => (source, (targetCounts.keys.toArray, targetCounts.values.toArray))}

  def fromMapToSeqRows(rowCounts: MapRow.Type): SeqRow.Type =
    rowCounts.map { 
        case (source, targetCounts) 
        => (source, targetCounts.toSeq) }
  
  def fromArrayToSeqRows(rowCounts: ArrayRow.Type): SeqRow.Type =
    rowCounts.map { 
        case (source, (columns,values)) 
        =>  (source, columns.zip(values)) }
  
  def fromArrayToMapRows(rowCounts: ArrayRow.Type): MapRow.Type =
    rowCounts.map { 
        case (source, (columns,values)) 
        =>  (source, columns.zip(values).toMap)}
  
  def fromCoordinateToSeqRows(matrix: Coordinate.Type): SeqRow.Type = 
    matrix.
      map { 
          case ((row, col), value) 
          => (row, (col, value)) }.
      groupByKey.
      map { 
          case (row, colValues) 
          => (row, colValues.toSeq.sortBy(_._1)) }
          
}