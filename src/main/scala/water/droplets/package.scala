package water

import water.fvec.DataFrame

import scala.util.{Failure, Success, Try}

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
      Try (!cols.map(name => { if (!df.names.contains(name)) false}).contains(false)) match {
        case Success(_) => {
          val indexes = df.find(cols)
          indexes.zipWithIndex.map(i =>
            {df.replace(df.find(cols(i._2)),df.vec(i._1).toEnum)
            df.update(null)}
          )
        }
        case Failure(_) => printf("One or several columns are not present in your DataFrame")
      }
    }

    /** *
      * Transform columns in enum columns
      * @param cols : Array[ Int ] containing all the indexes of enum columns
      */
    def colToEnum(cols: Array[Int]): Unit ={
      val colsNames = cols.map(i=>df.name(i))
      colToEnum(colsNames)
    }

    /** *
      * Rename a column of your DataFrame
      * @param oldName : Old name
      * @param newName : New name
      */
    def remane(oldName: String, newName: String): Unit ={
      val index = df.find(oldName)
      Try (index != -1) match {
        case Success(_) => remane(index, newName)
        case Failure(_) => println("Column missing")
      }
    }

    /** *
      * Rename a column of your DataFrame
      * @param index : Index of the column to rename
      * @param newName : New name
      */
    def remane(index: Int, newName: String): Unit ={
      val tmp = df.names
      Try(tmp.length+1 < index) match {
        case Success(_) => {
          tmp(index) = newName
          df._names = tmp
        }
        case Failure(t) => println(t.toString)
      }
    }
  }
}
