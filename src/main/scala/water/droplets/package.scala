package water

import water.fvec.DataFrame

/**
 * Created by B-COURBE on 21/04/2015.
 */
package object droplets {
  implicit class DataFrameOverride(df: DataFrame){
    def colToEnum(cols: Array[String]): Unit ={
      val indexes = df.find(cols)
      indexes.zipWithIndex.map(i =>
      {df.replace(df.find(cols(i._2)),df.vec(i._1).toEnum)
        df.update(null)}
      )
    }
  }
}
