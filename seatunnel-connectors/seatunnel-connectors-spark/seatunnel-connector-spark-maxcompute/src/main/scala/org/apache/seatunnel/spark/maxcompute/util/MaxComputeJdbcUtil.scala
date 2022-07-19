package org.apache.seatunnel.spark.maxcompute.util

import com.aliyun.datahub.common.util.RetryUtil
import com.aliyun.odps.OdpsType
import com.aliyun.odps.jdbc.utils.transformer.to.odps.ToOdpsTransformerFactory
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DatabaseMetaData, Date, DriverManager, ResultSet, Timestamp, Types}
import java.util.concurrent.Callable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class MaxComputeJdbcUtil{

}
object MaxComputeJdbcUtil {

  val ODPS_DRIVER = "com.aliyun.odps.jdbc.OdpsDriver"
  private def LOG: Logger = LoggerFactory.getLogger(classOf[MaxComputeJdbcUtil])

  def getJdbcConnection(address : String ,accessKeyId: String ,accessKeySecret: String ,project : String):Connection ={
    Class.forName(ODPS_DRIVER)
    val url = s"jdbc:odps:${address}?project=${project}&useProjectTimeZone=true"
    RetryUtil.executeWithRetry(new Callable[Connection]() {
      @throws[Exception]
      override def call: Connection = {
        DriverManager.getConnection(url,accessKeyId,accessKeySecret)
      }
    }, 3, 1000L, true)
  }

  def getFieldAndValue(rs : ResultSet): ListBuffer[(String, String)] = {
    val fieldAndValuesWithOrders = ListBuffer[(Int,String,String)]()
    while (rs.next()){
      val fieldAndValuesWithOrder = (rs.getInt("ordinal_position"),rs.getString("COLUMN_NAME"),rs.getString("TYPE_NAME").toLowerCase())
      println(fieldAndValuesWithOrder)
      fieldAndValuesWithOrders.append(fieldAndValuesWithOrder)
    }
    val listBuffer: ListBuffer[(String, String)] = fieldAndValuesWithOrders.sortBy(data => data._1).map(data => (data._2, data._3))
    listBuffer
  }

  def writePartitionOpds(partition: Iterator[Row] ,address : String ,accessKeyId: String ,accessKeySecret: String ,project : String,
                         table : String ,fieldNames: Array[String],batchSize : Int) ={
    val connection: Connection = getJdbcConnection(address, accessKeyId, accessKeySecret, project)
    val metaData: DatabaseMetaData = connection.getMetaData
    val catalog = connection.getCatalog
    val metaRs: ResultSet = metaData.getColumns(catalog, null, table, null)
    val fieldAndValues = getFieldAndValue(metaRs)
    val sql = s"insert into ${table} values(${fieldAndValues.map(_ => "?").mkString(",")})"
    LOG.info(s"execute sql : ${sql}")
    val preparedStatement = connection.prepareStatement(sql)
    var curSize = 0
    for(row <- partition){
      for(i <- fieldAndValues.indices){
        val fieldAndValue: (String, String) = fieldAndValues(i)
        val index = fieldNames.indexOf(fieldAndValue._1)
        if (index != -1) {
          LOG.info(s"compare : field=${fieldAndValue._2},value=${row.get(index).getClass.getName}")
          preparedStatement.setObject(i+1,row.get(index))
        }else {
          preparedStatement.setObject(i+1 , fieldAndValue._2 match {
            case "tinyint" => 0
            case "smallint" => 0
            case "int" => 0
            case "bigint" => 0.toLong
            case "float" => 0.0
            case "double" => 0.0
            case "decimal" => java.math.BigDecimal.valueOf(0.0)
            case "varchar" => ""
            case "string" => ""
            case "datetime" => new Timestamp(System.currentTimeMillis)
            case "timestamp" => new Timestamp(System.currentTimeMillis)
            case "boolean" => false
          })
        }
      }
      preparedStatement.addBatch()
      curSize=curSize+1
      if (curSize > batchSize){
        val ret = preparedStatement.executeBatch()
        assert(ret(0) == 1)
        assert(ret(1) == 1)
        curSize=curSize - batchSize
      }
    }
    if(curSize > 0) {
      val ret = preparedStatement.executeBatch()
      assert(ret(0) == 1)
      assert(ret(1) == 1)
    }
    try{
      preparedStatement.close()
      connection.close()
    }catch {
      case e: ArithmeticException => {
        LOG.error("Fail to close the connection,but the job have done successfully")
        throw e
      }
    }

  }



}
