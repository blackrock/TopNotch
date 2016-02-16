package com.bfm.topnotch.tnengine

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnectionManager, HConnection, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
 * A trait for persisting a string representing a report to a location accessible by an external UI system
 */
trait TnPersister {
  /**
   * Writes results of TopNotch command to a persistent storage location.
   * @param key The key used to reference the report in the database
   * @param reportStr A report represented as a string
   */
  def persistReport(key: String, reportStr: String)
}

/**
 * An implementation of TnPersister for persisting reports to HDFS
 * @param hdfsPath The directory on HDFS to write the reports to
 */
case class TnHDFSPersister(hdfsPath: String = "topnotch/") extends TnPersister {
  def persistReport(key: String, reportStr: String): Unit = {
    val fileOut = TnHDFSPersister.FS.create(new Path(hdfsPath, key), true)
    fileOut.write(reportStr.getBytes())
    fileOut.close()
  }
}

/**
  * The constants used by TnHDFSPersister
  */
object TnHDFSPersister {
  lazy val FS = FileSystem.get(new Configuration())
}

/**
 * An implementation of TnPersister for persisting reports to HBase
 * @param hconn The hbase connection to use. Its left as a parameter for mocking out during testing
 */
class TnHBasePersister(hconn: Option[HConnection] = None) extends TnPersister {
  import TnHBasePersister._

  def persistReport(key: String, reportStr: String): Unit = {
    val table = hconn.getOrElse(HCONN).getTable(TABLE_NAME)
    val uniRecord = new Put(Bytes.toBytes(key))
    uniRecord.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_QUALIFIER), Bytes.toBytes(reportStr))
    table.put(uniRecord)
    table.close()
  }
}

/**
 * The constants used by TnHBasePersister
 */
object TnHBasePersister {
  val TABLE_NAME = "TopNotch"
  val COLUMN_FAMILY = "reports"
  val COLUMN_QUALIFIER = "qualifier"
  lazy val HCONN = HConnectionManager.createConnection(HBaseConfiguration.create)
}
