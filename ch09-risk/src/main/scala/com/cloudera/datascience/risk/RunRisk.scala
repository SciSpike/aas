/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.risk

import java.io.File
import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import breeze.plot._

import com.github.nscala_time.time.Implicits._

import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

import org.joda.time.DateTime

object RunRisk {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis
    val sc = new SparkContext(new SparkConf().setAppName("VaR").setMaster("local[*]"))
    val basedir = (if (args.length > 0) args(0) else ".") + "/"
    val (stocksReturns, factorsReturns) = readStocksAndFactors(basedir)
    plotDistribution(factorsReturns(2), "S&P 500 Historical Returns")
    plotDistribution(factorsReturns(3), "NASDAQ Composite Historical Returns")
    val numTrials = 10000000
    val parallelism = 1000
    val baseSeed = 1001L
    val trials = computeTrialReturns(stocksReturns, factorsReturns, sc, baseSeed, numTrials,
      parallelism)
    trials.cache()
    val valueAtRisk = fivePercentVaR(trials)
    val conditionalValueAtRisk = fivePercentCVaR(trials)
    println("VaR 5%: " + valueAtRisk)
    println("CVaR 5%: " + conditionalValueAtRisk)
    val varConfidenceInterval = bootstrappedConfidenceInterval(trials, fivePercentVaR, 100, .05)
    val cvarConfidenceInterval = bootstrappedConfidenceInterval(trials, fivePercentCVaR, 100, .05)
    println("VaR confidence interval: " + varConfidenceInterval)
    println("CVaR confidence interval: " + cvarConfidenceInterval)
    println("Kupiec test p-value: " + kupiecTestPValue(stocksReturns, valueAtRisk, 0.05))
    plotDistribution(trials, "Trial Returns")
    val end = System.currentTimeMillis
    println(s"Total time taken: ${(end - start)/1000/60.0} mins")
    sc.stop()
  }

  def computeTrialReturns(
      stocksReturns: Seq[Array[Double]],
      factorsReturns: Seq[Array[Double]],
      sc: SparkContext,
      baseSeed: Long,
      numTrials: Int,
      parallelism: Int): RDD[Double] = {
    // convert factor returns to matrix of Doubles (Array[Array[Double]])
    val factorMat = factorMatrix(factorsReturns)
    // use matrix to calculate Covariance matrix
    val factorCov = new Covariance(factorMat).getCovarianceMatrix().getData()
    // calculate means of returns for each factor
    val factorMeans = factorsReturns.map(factor => factor.sum / factor.size).toArray
    // featurize factor returns:
      // signed squares of each feature return,
      // signed square roots of each feature return, and
      // feature returns themselves
    val factorFeatures = factorMat.map(featurize)
    // calculate factor weights for each stock
    val factorWeights = computeFactorWeights(stocksReturns, factorFeatures)
    // share weights across Spark cluster
    val bInstruments = sc.broadcast(factorWeights)
    // generate different seeds so that our trials don't all end up with the same results
    val seeds = baseSeed until baseSeed + parallelism
    val seedRdd = sc.parallelize(seeds, parallelism)
    // main computation: run simulations and compute aggregate return for each
    seedRdd.flatMap(seed =>
      trialReturns(seed, numTrials / parallelism, bInstruments.value, factorMeans, factorCov))
  }

  def computeFactorWeights(
      stocksReturns: Seq[Array[Double]],
      factorFeatures: Array[Array[Double]]): Array[Array[Double]] = {
    // calculate multivariate linear regression for each stock
    val models = stocksReturns.map(linearModel(_, factorFeatures))
    // calculate factor weights for each stock using stock's linear regression model
    val factorWeights = Array.ofDim[Double](stocksReturns.length, factorFeatures.head.length+1)
    for (s <- 0 until stocksReturns.length) {
      factorWeights(s) = models(s).estimateRegressionParameters()
    }
    factorWeights
  }

  def featurize(factorReturns: Array[Double]): Array[Double] = {
    // given returns of a factor, prepend signed squares & square roots
    val signedSquares = factorReturns.map(x => math.signum(x) * x * x)
    val signedSquareRoots = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
    signedSquares ++ signedSquareRoots ++ factorReturns
  }

  def readStocksAndFactors(prefix: String): (Seq[Array[Double]], Seq[Array[Double]]) = {
    val start = new DateTime(2009, 10, 23, 0, 0)
    val end = new DateTime(2014, 10, 23, 0, 0)

    val rawStocks = readHistories(new File(prefix + "data/stocks/"))
      // keep only those stocks with 5 years + 2 weeks of history
      .filter(_.size >= 52*5*5+10)
    println(s"${rawStocks.length} raw stocks have been read")
    // trim dates & fill histories for stocks
    val stocks = rawStocks
      .map(trimToRegion(_, start, end))
      .map(fillInHistory(_, start, end))
    println(s"${stocks.length} stocks have been region-trimmed & history-filled")
    // trim dates & fill histories for factors
    val factorsPrefix = prefix + "data/factors/"
    val oilAndBonds = Array("crudeoil.tsv", "us30yeartreasurybonds.tsv").
      map(x => new File(factorsPrefix + x)).
      map(readInvestingDotComHistory)
    val sp500AndNasdaq = Array("^GSPC.csv", "^IXIC.csv").
      map(x => new File(factorsPrefix + x)).
      map(readYahooHistory)
    val factors = (oilAndBonds ++ sp500AndNasdaq).
      map(trimToRegion(_, start, end)).
      map(fillInHistory(_, start, end))
    println(s"${factors.length} factors have been region-trimmed & history-filled")
    // calculate 2-week returns
    val stockReturns = stocks.map(twoWeekReturns)
    val factorReturns = factors.map(twoWeekReturns)
    println(s"two-week returns calculated")

    (stockReturns, factorReturns)
  }

  def trialReturns(
      seed: Long,
      numTrials: Int,
      instruments: Seq[Array[Double]],
      factorMeans: Array[Double],
      factorCovariances: Array[Array[Double]]): Seq[Double] = {
    // new RNG from seed
    val rand = new MersenneTwister(seed)
    // calculate multivariate normal distribution
    val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans,
      factorCovariances)
    // calculate returns from random sample
    val trialReturns = new Array[Double](numTrials)
    for (i <- 0 until numTrials) {
      val trialFactorReturns = multivariateNormal.sample()
      val trialFeatures = RunRisk.featurize(trialFactorReturns)
      trialReturns(i) = trialReturn(trialFeatures, instruments)
    }
    trialReturns
  }

  /**
   * Calculate the full return of the portfolio under particular trial conditions.
   */
  def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]): Double = {
    var totalReturn = 0.0
    for (instrument <- instruments) {
      totalReturn += instrumentTrialReturn(instrument, trial)
    }
    totalReturn / instruments.size
  }

  /**
   * Calculate the return of a particular instrument under particular trial conditions.
   */
  def instrumentTrialReturn(instrument: Array[Double], trial: Array[Double]): Double = {
    var instrumentTrialReturn = instrument(0)
    var i = 0
    while (i < trial.length) {
      instrumentTrialReturn += trial(i) * instrument(i+1)
      i += 1
    }
    instrumentTrialReturn
  }

  def twoWeekReturns(history: Array[(DateTime, Double)]): Array[Double] = {
    // returns collection of arrays based on 2-week (10-day) windows
    history.sliding(10)
      .map { window => // for each 2-week window
      // change is close at end of current window
      val next = window.last._2
      // basis is close at end of previous window
      val prev = window.head._2
      // calculate return as % change from previous
      (next - prev) / prev
    }.toArray
  }

  def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]])
  : OLSMultipleLinearRegression = {
    val regression = new OLSMultipleLinearRegression()
    regression.newSampleData(instrument, factorMatrix)
    regression
  }

  def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](histories.head.length)
    for (i <- 0 until histories.head.length) {
      mat(i) = histories.map(_(i)).toArray
    }
    mat
  }

  def readHistories(dir: File): Seq[Array[(DateTime, Double)]] = {
    val files = dir.listFiles()
    files.flatMap(file => {
      try {
        Some(readYahooHistory(file))
      } catch {
        case _: Exception => None
      }
    })
  }

  def trimToRegion(history: Array[(DateTime, Double)], start: DateTime, end: DateTime)
  : Array[(DateTime, Double)] = {
    // keep data in desired date range only
    var trimmed = history.dropWhile(_._1 < start).takeWhile(_._1 <= end)
    // if first element's date not desired start
    if (trimmed.head._1 != start) { 
      // prepend entry for start using current head's price
      trimmed = Array((start, trimmed.head._2)) ++ trimmed
    }
    // if last element's date not desired end
    if (trimmed.last._1 != end) {
      // append entry for end using current end's price
      trimmed = trimmed ++ Array((end, trimmed.last._2))
    }
    trimmed
  }

  /**
   * Given a timeseries of values of an instruments, returns a timeseries between the given
   * start and end dates with all missing weekdays imputed. Values are imputed as the value on the
   * most recent previous given day.
   */
  def fillInHistory(history: Array[(DateTime, Double)], start: DateTime, end: DateTime)
  : Array[(DateTime, Double)] = {
    var cur = history
    val filled = new ArrayBuffer[(DateTime, Double)]()
    var curDate = start
    while (curDate < end) {
      if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
        cur = cur.tail
      }
      filled += ((curDate, cur.head._2))
      curDate += 1.days
      // skip weekends
      if (curDate.dayOfWeek().get > 5) curDate += 2.days
    }
    filled.toArray
  }

  def readInvestingDotComHistory(file: File): Array[(DateTime, Double)] = {
    println(s"Reading Investing.com file ${file.getName}")
    val format = new SimpleDateFormat("MMM d, yyyy", Locale.ENGLISH)
    val lines = Source.fromFile(file).getLines().toSeq
    lines.map(line => {
      val cols = line.split('\t')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value) // maps each line to a tuple
    }).reverse.toArray
  }

  /**
   * Reads a history in the Yahoo format
   */
  def readYahooHistory(file: File): Array[(DateTime, Double)] = {
    println(s"Reading Yahoo file ${file.getName}")
    val format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
    val lines = Source.fromFile(file).getLines().toSeq
    lines.tail.map(line => { // tail skips header row
      val cols = line.split(',')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value) // maps each line to a tuple
    }).reverse.toArray
  }

  def plotDistribution(samples: Array[Double], title:String): Figure = {
    val min = samples.min
    val max = samples.max
    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val densities = KernelDensity.estimate(samples, domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
    p.title = title
    f
  }

  def plotDistribution(samples: RDD[Double], title:String): Figure = {
    val stats = samples.stats()
    val min = stats.min
    val max = stats.max
    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val densities = KernelDensity.estimate(samples, domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
    p.title = title
    f
  }

  def fivePercentVaR(trials: RDD[Double]): Double = {
    val topLosses = trials.takeOrdered(math.max(trials.count().toInt / 20, 1))
    topLosses.last // 5th worst loss
  }

  def fivePercentCVaR(trials: RDD[Double]): Double = {
    val topLosses = trials.takeOrdered(math.max(trials.count().toInt / 20, 1))
    topLosses.sum / topLosses.length // average of 5 worst losses
  }

  def bootstrappedConfidenceInterval(
      trials: RDD[Double],
      computeStatistic: RDD[Double] => Double,
      numResamples: Int,
      pValue: Double): (Double, Double) = {
    // for each sample, call compute function
    val stats = (0 until numResamples).map { i =>
      val resample = trials.sample(true, 1.0)
      computeStatistic(resample)
    }.sorted
    // take subset of samples based on p-value
    val lowerIndex = (numResamples * pValue / 2 - 1).toInt
    val upperIndex = math.ceil(numResamples * (1 - pValue / 2)).toInt
    // return interval
    (stats(lowerIndex), stats(upperIndex))
  }

  def countFailures(stocksReturns: Seq[Array[Double]], valueAtRisk: Double): Int = {
    var failures = 0
    for (i <- 0 until stocksReturns(0).size) {
      val ret = stocksReturns.map(_(i)).sum
      if (ret < valueAtRisk) {
        failures += 1
      }
    }
    failures
  }

  def kupiecTestStatistic(total: Int, failures: Int, confidenceLevel: Double): Double = {
    val failureRatio = failures.toDouble / total
    val logNumer = (total - failures) * math.log1p(-confidenceLevel) +
      failures * math.log(confidenceLevel)
    val logDenom = (total - failures) * math.log1p(-failureRatio) +
      failures * math.log(failureRatio)
    -2 * (logNumer - logDenom)
  }

  def kupiecTestPValue(
      stocksReturns: Seq[Array[Double]],
      valueAtRisk: Double,
      confidenceLevel: Double): Double = {
    val failures = countFailures(stocksReturns, valueAtRisk)
    val total = stocksReturns(0).size
    val testStatistic = kupiecTestStatistic(total, failures, confidenceLevel)
    1 - new ChiSquaredDistribution(1.0).cumulativeProbability(testStatistic)
  }

}
