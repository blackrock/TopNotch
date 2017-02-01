package com.bfm.topnotch.tnengine

import java.net.URL
import java.util

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicResponseHandler, HttpClients}

/**
  * A trait for persisting a string representing a report to a location accessible by an external UI system
  */
trait TnWriter extends StrictLogging {
  /**
    * Writes results of TopNotch command to a persistent storage location.
    *
    * @param key       The key used to reference the report in the database
    * @param reportStr A report represented as a string
    */
  def writeReport(key: String, reportStr: String)
}

/**
  * The strings to use in configuration files to select the appropriate writer
  */
object TnWriterConfigStrings {
  val HBASE_CONF_STRING = "hbase"
  val HDFS_CONF_STRING = "hdfs"
  val REST_CONF_STRING = "rest"
}

/**
  * An implementation of TnWriter for writing reports to HDFS
  *
  * @param dest The directory on HDFS to write the reports to.
  */
case class TnHDFSWriter(dest: Option[String] = None) extends TnWriter {
  private val defaultDest = "topnotch/"
  def writeReport(key: String, reportStr: String): Unit = {
    val fileOut = TnHDFSWriter.FS.create(new Path(dest.getOrElse(defaultDest), key), true)
    fileOut.write(reportStr.getBytes())
    fileOut.close()
  }
}

/**
  * The constants used by TnHDFSWriter
  */
object TnHDFSWriter {
  lazy val FS = FileSystem.get(new Configuration())
}

/**
  * An implementation of TnWriter for writing reports to HBase
  *
  * @param hconn The hbase connection to use. Its left as a parameter for mocking out during testing
  */
class TnHBaseWriter(hconn: Option[HConnection] = None) extends TnWriter {

  import TnHBaseWriter._

  def writeReport(key: String, reportStr: String): Unit = {
    val table = hconn.getOrElse(HCONN).getTable(TABLE_NAME)
    val uniRecord = new Put(Bytes.toBytes(key))
    uniRecord.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_QUALIFIER), Bytes.toBytes(reportStr))
    table.put(uniRecord)
    table.close()
  }
}

/**
  * The constants used by TnHBaseWriter
  */
object TnHBaseWriter {
  val TABLE_NAME = "TopNotch"
  val COLUMN_FAMILY = "reports"
  val COLUMN_QUALIFIER = "qualifier"
  lazy val HCONN = HConnectionManager.createConnection(HBaseConfiguration.create)
}

/**
  * An implementation of TnWriter for writing reports to a REST API
  *
  * @param dest The URL of the API to write to
  */
case class TnRESTWriter(dest: String) extends TnWriter {
  def writeReport(key: String, reportStr: String): Unit = {
    val httpclient = HttpClients.createDefault()
    val httpPut = new HttpPut(new URL(new URL(dest), key).toURI)

    httpPut.setEntity(new StringEntity(reportStr))
    httpPut.setHeader("Accept", "application/json")
    httpPut.setHeader("Content-Type", "application/json")

    logger.info("Put Request " + httpPut)
    logger.info("Put Request Headers:")
    httpPut.getAllHeaders.foreach(arg => println(arg))
    logger.info("Put Request Entity:")
    httpPut.getEntity

    // send the post request
    val response = httpclient.execute(httpPut)
    logger.info("Put Response Headers for " + httpPut.getURI)
    response.getAllHeaders.foreach(arg => println(arg))
    logger.info("Put Body for " + httpPut.getURI)
    logger.info(new BasicResponseHandler().handleResponse(response))
  }
}
