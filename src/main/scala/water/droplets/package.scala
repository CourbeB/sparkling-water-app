package water

import water.fvec.DataFrame

/**
 * Created by B-COURBE on 21/04/2015.
 */
package object droplets {
  implicit class DataFrameOverride(df: DataFrame){

    /** *
      * Transform columns in enum columns
      * @param cols : Array[ String ] containing all the names of enum columns
      */
    def colToEnum(cols: Array[String]): Unit ={
      val indexes = df.find(cols)
      indexes.zipWithIndex.map(i =>
      {df.replace(df.find(cols(i._2)),df.vec(i._1).toEnum)
        df.update(null)}
      )
    }

    /** *
      * Transform columns in enum columns
      * @param cols : Array[ Int ] containing all the indexes of enum columns
      */
    def colToEnum(cols: Array[Int]): Unit ={
      val colsNames = cols.map(i=>df.name(i))
      val indexes = df.find(colsNames)
      indexes.zipWithIndex.map(i =>
      {df.replace(df.find(colsNames(i._2)),df.vec(i._1).toEnum)
        df.update(null)}
      )
    }

    /** *
      * Rename a column of your DataFrame
      * @param oldName : Old name
      * @param newName : New name
      */
    def remane(oldName: String, newName: String): Unit ={
      val index = df.find(oldName)
      val tmp = df.names
      tmp(index) = newName
      df._names = tmp
    }

    /** *
      * Rename a column of your DataFrame
      * @param index : Index of the column to rename
      * @param newName : New name
      */
    def remane(index: Int, newName: String): Unit ={
      val tmp = df.names
      tmp(index) = newName
      df._names = tmp
    }
  }
}
