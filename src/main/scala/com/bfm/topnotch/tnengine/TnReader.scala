package com.bfm.topnotch.tnengine

import java.io.File
import java.net.URL

import org.apache.commons.httpclient.HttpStatus
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

/**
  * A trait for reading the configuration files from an external source.
  * @param variableDictionary A dictionary for replacing variables in the configuration objects with values
  */
abstract class TnReader(variableDictionary: Map[String, String]) {

  implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Read the configurations from given configuration location.
    * @param configPath The configPath is the path to the configurations
    * @param referrer The configuration AST that referred to this one
    * @return the AST of the configuration
    */
  def readConfiguration(configPath: String, referrer: Option[JValue] = None) : JValue

  protected def replaceVariablesInConf(jsonConf: JValue): JValue = {
    parse(replaceVariablesInConf(compact(render(jsonConf))))
  }

  protected def replaceVariablesInConf(confString: String): String = {
    variableDictionary.foldLeft(confString){case (partiallyReplacedConfString, (variable, value)) =>
      partiallyReplacedConfString.replaceAllLiterally(s"$${${variable}}", value)}
  }
}
/**
  * An implementation of TnReader for reading configurations from a rest API.
  * @param baseURL The base url that the rest reader should read from. Reads will be done by appending configPaths to this url.
  * @param variableDictionary A dictionary for replacing variables in the configuration objects with values
  */
class TnRESTReader(baseURL: URL, variableDictionary: Map[String, String] = Map.empty) extends TnReader(variableDictionary) {

  /**
    * Get a JSON AST representing a configuration from a REST API
    * @param configPath The path to the configuration to load. This is relative to baseURL.
    * @param referrer The AST of the file that referenced the one at configPath. This is none if loading the plan.
    * Loading is done relative to referrer if it is present.
    * @return the AST of the file
    */
  def readConfiguration(configPath: String, referrer: Option[JValue] = None): JValue = {
    val httpclient = HttpClients.createDefault()
    val response = httpclient.execute(new HttpGet(new URL(baseURL, configPath).toURI))

    if (response.getStatusLine.getStatusCode == HttpStatus.SC_OK) {
      val inputStream = response.getEntity.getContent
      try {
        replaceVariablesInConf(parse(scala.io.Source.fromInputStream(inputStream).mkString))
      }
      finally {
        inputStream.close()
      }
    }
    else {
      throw new IllegalArgumentException(s"Cant find configurations at $configPath.")
    }
  }

}

/**
  * An implementation of TnReader for reading configurations from a file.
  * @param variableDictionary A dictionary for replacing variables in the configuration objects with values
  */
class TnFileReader(variableDictionary: Map[String, String] = Map.empty) extends TnReader(variableDictionary) {
  /**
    * Get a JSON AST representing a configuration from from a file. The file will either be loaded using a path relative
    * to the driver's working directory, if running in local or yarn-client mode, or it'll be loaded directly from the
    * driver's directory as the --files flag uploads it there in yarn-cluster mode.
    *
    * This handles the inconsistency of the --files flag with regard to whether the master is local or on an executor
    *
    * The field path is added to each AST so that if its a plan, it can refer to other commands relative to its own path
    *
    * @param configPath The path to the file to load
    * @param referrer The AST of the file that referenced the one at filePath. This is none if loading the plan.
    * Loading is done relative to referrer if it is present.
    * @return the AST of the file
    */
  def readConfiguration(configPath: String, referrer: Option[JValue] = None): JValue = {
    val localFile = if (referrer.isEmpty) new File(configPath) else new File((referrer.get \ "path").extract[String], configPath)
    val executorFile = new File(localFile.getName)
    // if the driver is running locally, get the file with the path provided by the users
    if (localFile.exists()) {
      replaceVariablesInConf(
        parse(localFile) merge JObject(JField("path", localFile.getAbsoluteFile.getParentFile.getAbsolutePath))
      )
    }
    // otherwise, try to get the file from the working directory
    else if (executorFile.exists()) {
      replaceVariablesInConf(
        parse(executorFile) merge JObject(JField("path", localFile.getAbsoluteFile.getParentFile.getAbsolutePath))
      )
    }
    // if neither of those work, try loading from the classpath, which allows reading text files from jars included
    // in the classpath
    else if (getClass.getClassLoader.getResource(configPath) != null) {
      replaceVariablesInConf(
        parse(scala.io.Source.fromURL(getClass.getClassLoader.getResource(configPath)).mkString(""))
          merge JObject(JField("path", "/"))
      )
    }
    else {
      throw new IllegalArgumentException(s"Can't find file $configPath.")
    }
  }
}
