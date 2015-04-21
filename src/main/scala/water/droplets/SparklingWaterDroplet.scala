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

import java.io.File

import hex.tree.gbm.GBM
import hex.{ModelMetricsBinomial, ModelMetrics, Model}
import hex.tree.gbm.GBMModel
import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.spark.h2o.{StringHolder, H2OContext}
import org.apache.spark.{SparkFiles, SparkContext, SparkConf}
import water.fvec.{Frame, DataFrame}
import water.util.Timer
import scala.collection.mutable

/**
 * Example of Sparkling Water based application.
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

    val uri = new java.net.URI(args(0))
    val table = new DataFrame(uri)

    //Set categorical column
    table.colToEnum(args(1).split(','))

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
    val predict = gbmModel.score(table)('predict)
    gTimer.stop("H2O : Predict")

    // Compute number of mispredictions with help of Spark API
    val tmp = new Frame(table.vec(Symbol(args(1))).toEnum)
    val trainRDD = asRDD[StringHolder](tmp)
    val predictRDD = asRDD[StringHolder](predict)

    //ADD
    //val trainMetricsGBM = ModelMetrics.getFromDKV(gbmModel, table).asInstanceOf[ModelMetricsBinomial].auc

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

    // println(trainMetricsGBM.accuracy)
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

