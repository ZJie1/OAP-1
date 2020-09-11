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

/*
 * Base interface for test suite.
 */
package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import sys.process._

abstract class OapTestSuite extends BenchmarkConfigSelector with OapPerfSuiteContext with Logging {

  // Class information
  case class OapBenchmarkTest(_name: String, _sentence: String, _verifyFunction: () => Unit, _profile: String = "Benchmark") {
    def name = _name
    def sql = _sentence
    def verification = _verifyFunction
    def profile = _profile
  }
  def activeConf: BenchmarkConfig = {
    if (_activeConf.isEmpty) {
      assert(false, "No active configuration found!")
    }
    _activeConf.get
  }

  def allTests(): Seq[OapBenchmarkTest] = testSet

  def runAll(repCount: Int): Unit = {
    testSet.foreach{
      run(_, repCount)
    }
    resToDF(_resultMap,spark)
  }

  def run(name: String, repCount: Int): Unit = {
    testSet.filter(_.name == name).foreach{
      run(_, repCount)
    }
    resToDF(_resultMap,spark)
  }

  def run(test: OapBenchmarkTest, repCount: Int): Unit = {
    logWarning(s"running ${test.name} ($repCount times) ...")
    val result = (1 to repCount).map{ _ =>
      dropCache()
      TestUtil.queryTime(spark.sql(test.sql).foreach{ _ => })
    }.toArray

    val prev: Seq[(String, Array[Int])] = _resultMap.getOrElse(test.name, Nil)
    val curr = prev :+ (activeConf.toString, result)
    _resultMap.put(test.name, curr)
    test.verification()
  }

case class resultset(conf:String,
                     T1_ms:String,
                     T2_ms:String,
                     T3_ms:String,
                     median_ms:String)

  def mkIDs(len: Int) ={
    var res = Seq("0")
    for( i <- 1 until len ){
      res = res ++ Seq(i.toString)
    }
    res
    //Seq(res.toString())
  }
  def resToDF(resultMap: mutable.LinkedHashMap[String, Seq[(String, Array[Int])]], spark: SparkSession)= {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.udf

    val stringify = udf((vs: Seq[String]) => s"""${vs.mkString(",")}""")

    val res = resultMap.toSeq
    var dfMap= Map("key"->spark.emptyDataFrame)
    if (res.nonEmpty) {
      res.foreach { result =>
        val header =
          Seq(Tabulator.truncate(result._1)) ++
            (1 to result._2(0)._2.length).map("T" + _ + "/ms") ++
            Seq("Median/ms")
        val content = result._2.map(x =>
          Seq(Tabulator.truncate(x._1)) ++
            x._2.map(_.toString) ++
            Seq(TestUtil.median(x._2).toString)
        )


        println(s"the header in the res is " + header) //list()
        println(s"the each value in the header is : ")
        header.foreach(x => println(x + " ,"))

        println(s"the content in the res is " + content) //list(list(string,string,string,string,string))

        content.foreach{ cont =>
          println("each content in the res is : ")
          println(cont) //list(string,string,string,string,string)
          //cont.toList

          println(s"each value of content is ")
          cont.foreach(x => print(x + " ;"))
        }

        val contentDf = content.toDF
        println(s"\n\n------------------the content df is : \n\n")
        contentDf.show
        //var resultDf = spark.emptyDataFrame

        val header2Df = header.toDF("confs")
        println(s"\n\n------------------the header df is : \n\n")
        header2Df.show

        //combine header and content to the Seq[Seq[String]]
        val comb = Seq(header) ++ content
        comb.toDF.show
        // split comb to repeatTimes +2 columns
        val colNums = result._2(0)._2.length + 2
        val attrs = Array.tabulate(colNums)(n => "col_" + n)

        var newDf = comb.toDF("res")
        attrs.zipWithIndex.foreach(x => {
          newDf = newDf.withColumn(x._1,$"res".getItem(x._2))
        })
        newDf.show

        val key = result._1

        val tmpDfMap = Map(key -> newDf)
        dfMap = dfMap ++ tmpDfMap
        tmpDfMap.keys.foreach{i =>
          println("key is :" + i)
          println("value is :" + tmpDfMap(i))
        }
/*        // write it to the csv file
        val resPath = "file:///home/sparkuser/GIT/OAP/resCsv/3.csv"
        newDf.withColumn("res", stringify($"res"))
          .write
          .option("header","true")
          .mode("append")
          .csv(resPath)

        // one line one csv
        val csvDf = newDf.drop("res")
        csvDf
          .write
          .format("com.databricks.spark.csv")
          .option("header","true")
          .mode("append")
          .save(s"myfile_$key 4.csv")

        //parquet file
        csvDf
          .write
          .option("header","true")
          .mode("append")
          .save(s"myfile_$key 5")

        // one line one csv
        csvDf
          .write
          .option("header","true")
          .mode("append")
          .csv(s"myfile_$key 2")*/

        content.foreach { cont1 =>
          val combinelist  = Seq(header, cont1)
          combinelist.toDF.show

          println(s"-------------the res of the each cont in the content   Seq")

          val cont2Df = cont1.toDF("times")
          //cont2Df.append(header2Df)
          //resultDf = resultDf.unionAll(cont2Df)
          print(cont2Df.dtypes)
          cont2Df.show
/*          val resultDf = header2Df.join(cont2Df)
          resultDf.show*/
          /*val resultDf = cont2Df.withColumn("test",header2Df.col(header.toString()))
          resultDf.show*/
          /*val resultDf1 = cont2Df.withColumn("test",header2DfAs.col("confs"))
          resultDf1.show*/
          /*println(s"\n\n--------------------being unionall, the resDf is ")
          resultDf.show*/
        }
      /*  println(s"\n\n--------------------After ALL, the resDf is ")
        resultDf.show*/
      }
      println(s"\n\n--------------------After ALL, the resDfMap is ")
      dfMap.keys.foreach{i =>
        println("key is :" + i)
        println("value is :" + dfMap(i))
      }

      /*val resDf = dfMap.toSeq.toDF
      resDf.show*/

      /*val resPath = "file:///home/sparkuser/GIT/OAP/resCsv/4"
      resDf.withColumn("res", stringify($"res"))
        .write
        .option("header","true")
        .mode("append")
        .csv(resPath)*/
    }
  }

  private var _activeConf: Option[BenchmarkConfig] = None
  def runWith(conf: BenchmarkConfig)(body: => Unit): Unit = {
    _activeConf = Some(conf)
    beforeAll(conf.allSparkOptions())
    if (prepare()){
      body
    } else {
      assert(false, s"$this checkCondition Failed!")
    }
    afterAll()
    _activeConf = None
  }

  /**
   * Prepare running env, include data check, various settings
   * of current(active) benchmark config.
   *
   * @return true if success
   */
  def prepare(): Boolean

  /**
   *  Final table may look like:
   *  +--------+--------+--------+--------+--------+
   *  |        |        |   T1   |   TN   |Avg/Med |
   *  +--------+--------+--------+--------+--------+
   *  |        |config1 |        |        |        |
   *  +  Test1 +--------+--------+--------+--------+
   *  |        |config2 |        |        |        |
   *  +--------+--------+--------+--------+--------+
   *  |        |config1 |        |        |        |
   *  +  Test2 +--------+--------+--------+--------+
   *  |        |config2 |        |        |        |
   *  +--------+--------+--------+--------+--------+
   *
   *  resultMap: (Test1 -> Seq( (config1, (1, 2, 3, ...)),
   *                            (config2, (1, 2, 3, ...))),
   *              Test2 -> Seq( (config1, (1, 2, 3, ...)),
   *                            (config2, (1, 2, 3, ...))),
   *              ...)
   */
  private val _resultMap: mutable.LinkedHashMap[String, Seq[(String, Array[Int])]] =
    new mutable.LinkedHashMap[String, Seq[(String, Array[Int])]]

  def resultMap = _resultMap

  protected def testSet: Seq[OapBenchmarkTest]
  protected def dropCache(): Unit = {
    val nodes = spark.sparkContext.getExecutorMemoryStatus.map(_._1.split(":")(0))
    nodes.foreach { node =>
      //      val dropCacheResult = Seq("bash", "-c", s"""ssh $node "echo 3 > /proc/sys/vm/drop_caches"""").!
      val dropCacheResult = Seq("bash", "-c", s"""ssh $node "sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'"""").!
      assert(dropCacheResult == 0)
    }
  }

}

object BenchmarkSuiteSelector extends Logging{

  private val allRegisterSuites = new ArrayBuffer[OapTestSuite]()

  def registerSuite(suite: OapTestSuite) = {
    allRegisterSuites.append(suite)
    logWarning(s"Register $suite")
  }

  def allSuites: Seq[OapTestSuite] = allRegisterSuites

  var wildcardSuite: Option[String] = None

  def build(name: String): Unit = wildcardSuite = Some(name)

  // TODO: regex support
  def selectedSuites(): Seq[OapTestSuite] = wildcardSuite match {
    case Some(name) =>allRegisterSuites.filter(_.toString.contains(name))
    case None => allRegisterSuites
  }
}

object BenchmarkTestSelector {
  private var wildcardTest: Seq[String] = Seq.empty

  def build(name: String): Unit = wildcardTest = name.split(';')

  def selectedTests(): Seq[String] = wildcardTest
}
