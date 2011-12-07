package edu.berkeley.cs.amplab

import spark.SparkContext
import spark.RDD
import SparkContext._

import Util._
import GoogleTrace._
import TraceUtil._

import edu.berkeley.cs.amplab.mapreduce.output._
import edu.berkeley.cs.amplab.mapreduce.input._

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat}
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Job => HadoopJob}

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable

import com.google.protobuf.Message

object Utilizations {
  // TODO(Charles Reiss): weight by length of measurements.
  def getPercentile(p: Double, s: IndexedSeq[Float]): Float = {
    val place = ((s.size - 1) * p)
    val low = s(place.floor.asInstanceOf[Int])
    val high = s(place.ceil.asInstanceOf[Int])
    val offset = place - place.floor
    return (low * (1.0 - offset) + high * offset).asInstanceOf[Float]
  }

  val PER_TASK_PERCENTILES = List(0.5, 0.9, 0.99, 1.0)
  val JOB_PERCENTILES = List(0.5, 0.99, 1.0)

  def getTaskUtilization(usage: Seq[TaskUsage]): Option[TaskUtilization] = {
    val result = TaskUtilization.newBuilder
    val firstUsage = usage.head
    result.setInfo(firstUsage.getTaskInfo)

    val taskInfos =
      usage.filter(u => u.hasTaskInfo && u.getTaskInfo.hasRequestedResources).
            map(_.getTaskInfo)
    if (taskInfos.size == 0)
      return None

    result.setStartTime(usage.map(_.getStartTime).min).
           setEndTime(usage.map(_.getEndTime).max).
           setRunningTime(usage.map(u => u.getEndTime - u.getStartTime).sum)

    val minReqBuilder = result.getMinRequestBuilder
    val maxReqBuilder = result.getMaxRequestBuilder

    minReqBuilder.setCpus(taskInfos.map(_.getRequestedResources.getCpus).min)
    maxReqBuilder.setCpus(taskInfos.map(_.getRequestedResources.getCpus).max)
    minReqBuilder.setMemory(taskInfos.map(_.getRequestedResources.getMemory).min)
    maxReqBuilder.setMemory(taskInfos.map(_.getRequestedResources.getMemory).max)

    val maxCpus = usage.map(_.getMaxResources.getCpus).sorted.toIndexedSeq
    val maxMemory = usage.map(_.getMaxResources.getMemory).sorted.toIndexedSeq
    val cpus = usage.map(_.getResources.getCpus).sorted.toIndexedSeq
    val memory = usage.map(_.getResources.getMemory).sorted.toIndexedSeq

    for (percentile <- PER_TASK_PERCENTILES) {
      val meanUsage = Resources.newBuilder.
        setCpus(getPercentile(percentile, cpus)).
        setMemory(getPercentile(percentile, memory))

      val maxUsage = Resources.newBuilder.
        setCpus(getPercentile(percentile, maxCpus)).
        setMemory(getPercentile(percentile, maxMemory))

      result.addUsagePercentile(percentile.asInstanceOf[Float]).
             addPercentileTaskUsage(maxUsage).
             addPercentileMeanTaskUsage(meanUsage)
    }

    Some(result.build)
  }

  def getJobUtilization(tasks: Seq[TaskUtilization]): JobUtilization = {
    import scala.collection.JavaConversions._
    val result = JobUtilization.newBuilder
    val firstTask = tasks.head
    result.setJobInfo(firstTask.getInfo.getJob)
    val numTasks = tasks.groupBy(_.getInfo.getTaskIndex).size
    result.setNumTasks(numTasks)

    val minReqBuilder = result.getMinRequestBuilder
    val maxReqBuilder = result.getMaxRequestBuilder
    minReqBuilder.setCpus(tasks.map(_.getMinRequest.getCpus).min).
                  setMemory(tasks.map(_.getMinRequest.getMemory).min)
    maxReqBuilder.setCpus(tasks.map(_.getMaxRequest.getCpus).max).
                  setMemory(tasks.map(_.getMaxRequest.getMemory).max)

    val percentiles = firstTask.getUsagePercentileList
    for ((p, i) <- percentiles.zipWithIndex) {
      val maxCpus = tasks.map(_.getPercentileTaskUsage(i).getCpus).sorted.toIndexedSeq
      val maxMemory = tasks.map(_.getPercentileTaskUsage(i).getMemory).sorted.toIndexedSeq
      val cpus = tasks.map(_.getPercentileMeanTaskUsage(i).getCpus).sorted.toIndexedSeq
      val memory = tasks.map(_.getPercentileMeanTaskUsage(i).getMemory).sorted.toIndexedSeq

      for (p2 <- JOB_PERCENTILES) {
        result.addTaskPercentile(p2.asInstanceOf[Float]).
               addUsagePercentile(p.asInstanceOf[Float]).
          addPercentileTaskUsage(Resources.newBuilder.
            setCpus(getPercentile(p2, maxCpus)).
            setMemory(getPercentile(p2, maxMemory))).
          addPercentileMeanTaskUsage(Resources.newBuilder.
            setCpus(getPercentile(p2, cpus)).
            setMemory(getPercentile(p2, memory)))
      }
    }

    result.build
  }
}
