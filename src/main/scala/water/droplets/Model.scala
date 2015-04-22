package water.droplets

import hex.deeplearning.DeepLearningModel
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import hex.tree.gbm.GBMModel
import hex.tree.gbm.GBMModel.GBMParameters
import hex.glm.GLMModel
import hex.glm.GLMModel.GLMParameters
import org.apache.spark.h2o.H2OContext
import water.fvec.DataFrame

/**
 * Created by B-COURBE on 21/04/2015.
 */
class Model {

  def GBMModel(train: DataFrame,
               test: DataFrame,
               response: String,
               ntrees:Int = 10,
               depth:Int = 6,
               loss: GBMParameters.Family,
               learning_rate: Float = 0.1f,
                min_sample_leaf: Int = 1)
              (implicit h2oContext: H2OContext) : GBMModel = {
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters
    import h2oContext._

    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._valid = test
    gbmParams._response_column = response
    gbmParams._loss = loss
    gbmParams._learn_rate = learning_rate
    gbmParams._ntrees = ntrees
    gbmParams._max_depth = depth
    gbmParams._min_rows = min_sample_leaf
    //
    gbmParams._balance_classes = false
    gbmParams._max_after_balance_size = 5.0f
    gbmParams._class_sampling_factors = null
    gbmParams._max_hit_ratio_k = 10
    gbmParams._score_each_iteration = false
    gbmParams._nbins = 10
    gbmParams._ignored_columns = null
    gbmParams._dropConsCols = true
    gbmParams._dropNA20Cols = false
    gbmParams._max_confusion_matrix_size = 10
    //

    val gbm = new GBM(gbmParams)
    val model = gbm.trainModel.get
    model
  }

  def GLM(train: DataFrame, test: DataFrame, response: String,
         family: GLMParameters.Family = GLMParameters.Family.gaussian)
         (implicit h2OContext: H2OContext): GLMModel = {
    import hex.glm.GLM
    import hex.glm.GLMModel.GLMParameters
    import h2OContext._

    val glmParams = new GLMParameters()
    glmParams._train = train
    glmParams._valid = test
    glmParams._response_column = response
    //glmParams._family = family

    val glm = new GLM(glmParams)
    val model = glm.trainModel.get
    model
  }


  def DLModel(train: DataFrame, test: DataFrame, response: String,
              epochs: Int = 10, l1: Double = 0.0001, l2: Double = 0.0001,
              activation: Activation = Activation.RectifierWithDropout, hidden:Array[Int] = Array(200,200))
             (implicit h2oContext: H2OContext) : DeepLearningModel = {
    import hex.deeplearning.DeepLearning
    import hex.deeplearning.DeepLearningModel.DeepLearningParameters
    import h2oContext._

    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._valid = test
    dlParams._response_column = response
    dlParams._epochs = epochs
    dlParams._l1 = l1
    dlParams._l2 = l2
    dlParams._activation = activation
    dlParams._hidden = hidden

    // Create a job
    val dl = new DeepLearning(dlParams)
    val model = dl.trainModel.get
    model
  }
}
