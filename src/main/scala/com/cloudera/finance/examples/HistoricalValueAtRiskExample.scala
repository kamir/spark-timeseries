/**
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.finance.examples

import breeze.linalg.DenseVector

import com.cloudera.finance.{Util, YahooParser}
import com.cloudera.finance.risk.{FilteredHistoricalFactorDistribution,
  LinearInstrumentReturnsModel, ValueAtRisk}
import com.cloudera.sparkts.{GARCH, TimeSeriesFilter, TimeSeriesRDD}
import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts.TimeSeriesRDD._

import com.github.nscala_time.time.Imports._

import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

import org.apache.spark.{SparkConf, SparkContext}

import org.joda.time.DateTime

import ValueAtRisk._
import Util._

object HistoricalValueAtRiskExample {
  def main(args: Array[String]): Unit = {
    // Read parameters
    val numTrials = if (args.length > 0) args(0).toInt else 10000
    val parallelism = if (args.length > 1) args(1).toInt else 10
    val factorsDir = if (args.length > 2) args(2) else "factors/"
    val instrumentsDir = if (args.length > 3) args(3) else "instruments/"

    // Initialize Spark
    val conf = new SparkConf().setMaster("local").setAppName("Historical VaR")
    val sc = new SparkContext(conf)

    def loadTS(inputDir: String, lower: DateTime, upper: DateTime): TimeSeriesRDD[String] = {
      val histories = YahooParser.yahooFiles(instrumentsDir, sc)
      histories.cache()
      val start = histories.map(_.index.first).takeOrdered(1).head
      val end = histories.map(_.index.last).top(1).head
      val dtIndex = uniform(start, end, 1.businessDays)
      val tsRdd = timeSeriesRDD(dtIndex, histories).
        filter(_._1.endsWith("Open")).
        filterStartingBefore(lower).filterEndingAfter(upper)
      tsRdd.difference(10)
    }

    val year2000 = nextBusinessDay(new DateTime("2000-1-1"))
    val year2010 = nextBusinessDay(year2000 + 10.years)

    val instrumentReturns = loadTS(instrumentsDir, year2000, year2010)
    val factorReturns = loadTS(factorsDir, year2000, year2010).collectAsTimeSeries()

    // Fit factor return -> instrument return predictive models
    val factorObservations = factorReturns.observations()
    val linearModels = instrumentReturns.mapValues { series =>
      val regression = new OLSMultipleLinearRegression()
      regression.newSampleData(series.toArray, factorObservations)
      regression.estimateRegressionParameters()
    }.map(_._2).collect()
    val instrumentReturnsModel = new LinearInstrumentReturnsModel(arrsToMat(linearModels.iterator))

    // Fit an AR(1) + GARCH(1, 1) model to each factor
    val garchModels = factorReturns.mapSeries(GARCH.fitModel(_)._1)
    val iidFactorReturns = factorReturns.univariateSeriesIterator.zip(garchModels.iterator).map {
      case (history, model) =>
        model.removeTimeDependentEffects(history, DenseVector.zeros[Double](history.length))
    }

    // Generate an RDD of simulations
    val rand = new MersenneTwister()
    val factorsDist = new FilteredHistoricalFactorDistribution(rand, iidFactorReturns.toArray,
      garchModels.asInstanceOf[Array[TimeSeriesFilter]])
    val returns = simulationReturns(0L, factorsDist, numTrials, parallelism, sc,
      instrumentReturnsModel)
    returns.cache()

    // Calculate VaR and expected shortfall
    val pValues = Array(.01, .03, .05)
    val valueAtRisks = valueAtRisk(returns, pValues)
    println(s"Value at risk at ${pValues.mkString(",")}: ${valueAtRisks.mkString(",")}")

    val expectedShortfalls = expectedShortfall(returns, pValues)
    println(s"Expected shortfall at ${pValues.mkString(",")}: ${expectedShortfalls.mkString(",")}")
  }
}
