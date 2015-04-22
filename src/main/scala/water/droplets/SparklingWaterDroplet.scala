/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package water.droplets

import hex.tree.gbm.GBM
import hex.{ModelMetricsMultinomial, ModelMetricsBinomial, ModelMetrics}
import hex.tree.gbm.GBMModel
import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.spark.h2o.{StringHolder, H2OContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkFiles, SparkContext, SparkConf}
import water.fvec.{Frame, DataFrame}
import water.util.Timer
import scala.collection.mutable

/**
 * Example of Sparkling Water GBM application, need to be adapted
 * Param :
 * Args(0) : Absolute path to your data ("hdfs://mycluster/user/bcourbe/data/prostate.csv")
 * Args(1) : Name of the target column
 * Args(2) : Number of trees
 * Args(3) : Max depth
 */
object SparklingWaterDroplet {

  def main(args: Array[String]) {

    // Create Spark Context
    val conf = configure("Sparkling Water Test")
    val sc = new SparkContext(conf)

    implicit val gTimer = new GTimer

    // Create H2O Context
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._
    implicit val sqlContext = new SQLContext(sc)

    val uri = new java.net.URI(args(0))
    val table = new DataFrame(uri)

    //Set target as categorical column
    table.colToEnum(Array(args(1)))

    //Remove ID column
    table.remove("ID")
    table.update(null)

    // Build GBM model
    val gbmParams = new GBMParameters()
    gbmParams._train = table
    gbmParams._response_column = Symbol(args(1))
    gbmParams._ntrees = args(2).toInt
    gbmParams._max_depth = args(3).toInt
    gbmParams._loss = GBMParameters.Family.bernoulli

    val gbm = new GBM(gbmParams)
    gTimer.start()
    val gbmModel = gbm.trainModel.get
    gTimer.stop("H2O : Training")

    // Make prediction on train data
    gTimer.start()
    val result = gbmModel.score(table)
    val predict = result('predict)
    gTimer.stop("H2O : Predict")

    // Compute number of mispredictions with help of Spark API
    val trainRDD = asRDD[StringHolder](new Frame(table.vec(Symbol(args(1)))))
    val predictRDD = asRDD[StringHolder](predict)

    // Compute binomialMetrics
    val trainMetricsGBM = ModelMetricsBinomial.getFromDKV(gbmModel, table)

    // Make sure that both RDDs has the same number of elements
    assert(trainRDD.count() == predictRDD.count)
    val numMispredictions = trainRDD.zip(predictRDD).filter(i => {
      val act = i._1
      val pred = i._2
      act.result != pred.result
    }).collect()

    println(
      s"""
         |Number of mispredictions: ${numMispredictions.length}

          |
         |Mispredictions:
         |
         |actual X predicted
         |------------------
         |${numMispredictions.map(i => i._1.result.get + " X " + i._2.result.get).mkString("\n")}
       """.stripMargin)

    // Print different metrics
    println(trainMetricsGBM.cm.toASCII)
    println(trainMetricsGBM._mse)
    println(trainMetricsGBM.auc.accuracy)

    // Export Dataframe as SchemaRDD and use sql functions
    result._names = Array("predict","prob0","prob1")
    val schema  = asSchemaRDD(result)
    sqlContext.registerRDDAsTable(schema,"schema")
    sqlContext.sql("SELECT predict, prob1 FROM schema ORDER BY prob1 DESC ").take(10).foreach(println)

    // Shutdown application
    sc.stop()
  }

  def configure(appName:String = "Sparkling Water Demo"):SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
    conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "yarn-cluster"))
    conf
  }

}

class GTimer {
  type T = (String, String)
  val timeList = new mutable.Queue[T]()
  var t: Timer = _

  def start(): GTimer = {
    t = new Timer
    this
  }

  def stop(roundName: String): GTimer = {
    val item = roundName -> t.toString
    timeList += item
    t = null
    this
  }

  override def toString: String = {
    timeList.map(p => s"   * ${p._1} : takes ${p._2}").mkString("------\nTiming\n------\n", "\n", "\n------")
  }
}

