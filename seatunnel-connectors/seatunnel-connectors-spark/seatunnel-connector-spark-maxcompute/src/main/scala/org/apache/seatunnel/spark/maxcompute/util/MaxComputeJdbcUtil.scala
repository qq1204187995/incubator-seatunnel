package org.apache.seatunnel.spark.maxcompute.util

import com.aliyun.datahub.common.util.RetryUtil
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
import java.util.concurrent.Callable

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

  def getFieldAndValue(fieldNames: Array[String]): (String ,String) = {
    val fields = fieldNames.mkString(",")
    val values = fieldNames.map(_ => "?").mkString(",")
    (fields , values)
  }

  def writePartitionOpds(partition: Iterator[Row] ,address : String ,accessKeyId: String ,accessKeySecret: String ,project : String,
                         table : String ,fieldNames: Array[String]) ={
    val connection = getJdbcConnection(address, accessKeyId, accessKeySecret, project)
    val fieldAndValue: (String, String) = getFieldAndValue(fieldNames)
    val statement = connection.createStatement()
    println("select column_name from information_schema.columns where table_schema='%s' and table_name = '%s'".format(project.toLowerCase,table.toLowerCase()))
    val set = statement.executeQuery("select column_name,COLUMN_DEFAULT from information_schema.columns where table_schema='%s' and table_name = '%s'".format(project.toLowerCase,table.toLowerCase()))
    val sb = new StringBuffer()
    var value=new AnyRef
    while (set.next()){
      sb.append(set.getString("column_name")+",")
      value = set.getObject(2)
    }
    val allField = sb.deleteCharAt(sb.length() - 1).toString
    val allFieldArray: Array[String] = allField.split(",")
    val sql = s"insert into ${table} values(${allFieldArray.map(_ => "?").mkString(",")})"
    LOG.info(s"execute sql : ${sql}")
    val preparedStatement = connection.prepareStatement(sql)
    for(row <- partition){
      for(i <- allFieldArray.indices){
        val field = allFieldArray(i);
        val index = fieldNames.indexOf(field)
        if (index != -1) {
          preparedStatement.setObject(i+1,row.get(index))
        }
      }
      preparedStatement.addBatch()
    }
    preparedStatement.executeBatch()
  }
}
