/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource
import org.apache.flink.api.common.functions._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.functions.MapFunction


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
  */

object WikipediaEditsWindow {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val edits = env.addSource(new WikipediaEditsSource())

    val keyedEdits = edits
                      .keyBy(_.getUser)

    val result = keyedEdits
        .timeWindow(Time.seconds(2))
        .aggregate(new foldWiki)
        //.print()
        .map(_.toString())
        .addSink(new FlinkKafkaProducer08[String]("localhost:9092", "wiki-result", new SimpleStringSchema()))
    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */
    //println(result)
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}

class foldWiki extends AggregateFunction[WikipediaEditEvent, (Long, String, Int), (Long, String, Int)] {

  override def merge(acc: (Long, String, Int), acc1: (Long, String, Int)): (Long, String, Int) = (acc1._1, acc._2, acc._3 + acc1._3)
  override def getResult(acc: (Long, String, Int)): (Long, String, Int) = acc
  override def createAccumulator(): (Long, String, Int) = (0L, "", 0)
  val acc: (Long, String, Int) = createAccumulator()


  /*override def add(event: WikipediaEditEvent, acc: (String, Int) = acc): (String, Int) = acc match {

    case ("", 0) => (event.getUser, event.getByteDiff)
    case _ => merge(acc, (event.getUser, event.getByteDiff))

  }*/
  override def add(event: WikipediaEditEvent, acc: (Long, String, Int) = acc): (Long, String, Int) = acc match {

    case (_, id, _) if id == event.getUser => merge(acc, (event.getTimestamp, event.getUser, event.getByteDiff))
    case _ => (event.getTimestamp, event.getUser, event.getByteDiff)
  }



}
