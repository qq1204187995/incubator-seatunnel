package org.apache.seatunnel.spark.maxcompute.sink

import com.aliyun.odps.TableSchema
import com.aliyun.odps.data.Record
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.{SparkBatchSink, SparkBatchSource}
import org.apache.spark.aliyun.odps.OdpsOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.sql.DriverManager

class MaxCompute extends SparkBatchSink{
  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val accessKeyId = config.getString("accessKeyId")
    val accessKeySecret = config.getString("accessKeySecret")
    val project = config.getString("project")
    val table = config.getString("table")
    val urls = Seq("http://service.cn-shanghai.maxcompute.aliyun.com/api", "http://dt.cn-shanghai.maxcompute.aliyun.com")
    // 以内网地址为例。
    val values = data.schema.fieldNames.map(_ => "?").mkString(",")
    val url = s"jdbc:odps:${urls(0)}?project=${project}&useProjectTimeZone=true"
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver")
    val connection = DriverManager.getConnection(url,accessKeyId,accessKeySecret)
    val preparedStatement = connection.prepareStatement(s"insert into ${table} values(?,?,?)")
    preparedStatement.setObject(1,10)
    preparedStatement.setObject(2,"fqzzzz")
    preparedStatement.setObject(3,18)
    preparedStatement.executeUpdate()
//    val spark = data.sparkSession
//    val sc = spark.sparkContext
//    val odpsOps: OdpsOps = OdpsOps(sc, accessKeyId, accessKeySecret, urls(0), urls(1))
//    import spark.implicits._
//    val field: StructField = data.schema(0)
//    val dataType: DataType = field.dataType
////    val rdd: RDD[Row] = data.map(e => {})
//    val resultData: RDD[String] = data.map((e: Row) => s"${e.get(0)},${e.get(1)},38").rdd
//    odpsOps.saveToTable(project, table, resultData, write)
  }


  def write(s: String, emptyReord: Record, schema: TableSchema): Unit = {
    val columns = schema.getColumns
    val r = emptyReord
    val fields = s.split(",")
    for (i <- 0 to columns.size()){
      r.set(i , fields(i).to)
    }
    r.set(0, fields(0))
    r.set(1, fields(1))
    r.set(2, fields(2).toInt)
  }


  /**
   * This is a lifecycle method, this method will be executed after Plugin created.
   *
   * @param env environment
   */
  override def prepare(env: SparkEnvironment): Unit = {

  }

  override def checkConfig(): CheckResult = {
    //checkAllExists(config ,"")
    CheckResult.success()
  }

  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "MaxCompute"
}
