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
package org.apache.spark.sql.suites

import org.apache.spark.{SparkContext, SparkStageInfo, SparkStatusTracker}
import org.apache.spark.sql._
import org.apache.spark.sql.internal.oap.OapConf

object simpleSuite
  extends OapTestSuite with OapPerfSuiteContext with ParquetOnlyConfigSet{
  override protected def getAppName: String = "simpleSuite"

  val table = "store_sales"

  val attr = "ss_item_sk"

  def databaseName = "tpcds_10g_snappy"

  private def isDataBaseReady: Boolean = {
    val dbCandidates = spark.sqlContext.sql(s"show databases").collect()
    if (dbCandidates.exists(_.getString(0) == databaseName)) {
      spark.sqlContext.sql(s"USE $databaseName")
      true
    } else {
      logError(s"$dbCandidates does not contain $databaseName!")
      false
    }
  }

  private def isTableReady: Boolean = true

  private def isDataReady(): Boolean = isDataBaseReady && isTableReady

  override def beforeAll(conf: Map[String, String] = Map.empty): Unit = {
    super.beforeAll(conf)
  }

  private def setRunningParams(): Boolean = {
    val conf = activeConf
//    if (conf.getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE) == "false"){
//      spark.sqlContext.conf.setConf(OapConf.OAP_ENABLE_OINDEX, false)
//    }

    true
  }

  override def prepare(): Boolean = {
    if (isDataReady()) {
      setRunningParams()
    } else {
      sys.error("ERROR: Data is not ready!")
      false
    }
  }

    /**
     * (name, sql sentence, TODO: profile, etc)
     */
    override def testSet = Seq(
      OapBenchmarkTest("simple test",
        s"SELECT SUM($attr) FROM $table",verifyFunction),
      OapBenchmarkTest("JIRA 333 ID 444",
        s"SELECT AVG($attr) FROM $table",verify2)
     )

   def verifyFunction(): Unit = {
    def sc : SparkContext = spark.sparkContext
    def statusTracker : SparkStatusTracker = sc.statusTracker
    def stageInfo : Option[SparkStageInfo] = statusTracker.getStageInfo(0)
    val tasknums : Int  = stageInfo.get.numTasks()
    logWarning(s"tasknum for stage 0 is $tasknums")
    assert(tasknums == 4)
    logWarning(s"assert is successfully")
  }

  def verify2(): Unit = {
    // add assert here

    def sc : SparkContext = spark.sparkContext
    def statusTracker : SparkStatusTracker = sc.statusTracker
    def stageInfo : Option[SparkStageInfo] = statusTracker.getStageInfo(2)
    val tasknums : Int  = stageInfo.get.numTasks()
    logWarning(s"tasknum for stage 2 is $tasknums")
    assert(tasknums == 1)
    logWarning(s"assert is successfully")
  }
}
