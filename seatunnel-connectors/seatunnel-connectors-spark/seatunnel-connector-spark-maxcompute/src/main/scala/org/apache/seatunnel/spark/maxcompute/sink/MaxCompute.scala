package org.apache.seatunnel.spark.maxcompute.sink

import com.aliyun.odps.TableSchema
import com.aliyun.odps.data.Record
import com.aliyun.openservices.log.common.Logs.Log
import org.apache.seatunnel.common.config.CheckConfigUtil.{checkAllExists, checkAtLeastOneExists}
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.{SparkBatchSink, SparkBatchSource}
import org.apache.seatunnel.spark.maxcompute.util.MaxComputeJdbcUtil
import org.apache.spark.aliyun.odps.OdpsOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}

class MaxCompute extends SparkBatchSink{

  private var accessKeyId:String = _
  private var accessKeySecret:String = _
  private var project:String = _
  private var table:String = _
  private var address:String = _
  private var batchSize:Int = _
  private def LOG: Logger = LoggerFactory.getLogger(classOf[MaxCompute])


  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val fieldNames: Array[String] = data.schema.fieldNames
    data.foreachPartition((partition: Iterator[Row]) =>{
      MaxComputeJdbcUtil.writePartitionOpds(partition, address, accessKeyId, accessKeySecret, project, table, fieldNames, batchSize)
    })
  }



  /**
   * This is a lifecycle method, this method will be executed after Plugin created.
   *
   * @param env environment
   */
  override def prepare(env: SparkEnvironment): Unit = {
    accessKeyId = config.getString("accessKeyId")
    accessKeySecret = config.getString("accessKeySecret")
    project = config.getString("project")
    table = config.getString("table")
    address = config.getString("address")
    batchSize = if (config.hasPath("batchSize"))config.getInt("batchSize") else 0
    if (batchSize < 0){
      LOG.warn(s"Illegal input : batchSize : ${batchSize}<0 ,already set it to 0")
      batchSize = 0
    }

  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config ,"accessKeyId","accessKeySecret","project","table","address")
  }

  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "MaxCompute"
}
