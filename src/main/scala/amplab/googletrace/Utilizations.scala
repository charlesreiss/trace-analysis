package amplab.googletrace

import spark.SparkContext
import spark.RDD
import SparkContext._

import Util._
import Protos._
import TraceUtil._
import Convert._ // for out*

import amplab.googletrace.mapreduce.output._
import amplab.googletrace.mapreduce.input._

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
    result.addTaskSamples(firstTask.getInfo)

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

    result.setRunningTime(tasks.map(_.getRunningTime).sum).
           setStartTime(tasks.map(_.getStartTime).min).
           setEndTime(tasks.map(_.getEndTime).max)

    result.build
  }
  
  def findTaskUtilizations(tasks: RDD[TaskUsage]): RDD[TaskUtilization] = {
    return tasks.map(keyByTask).groupByKey.
        flatMap(kv => {
          val u = getTaskUtilization(kv._2)
          if (u.isEmpty)
            None
          else
            Some(u.get)
        })
  }

  def findJobUtilizations(tasks: RDD[TaskUsage]): RDD[JobUtilization] = {
    return tasks.map(keyByTask).groupByKey.
        flatMap(kv => {
          val u = getTaskUtilization(kv._2)
          if (u.isEmpty)
            None
          else
            Some(kv._1._1 -> u.get)
        }).groupByKey.map(kv => getJobUtilization(kv._2))
  }

  def normalizeTime(t: Long): Long = t / (300 * 1000L * 1000L)

  def keyByJob(j: JobUtilization): (Long, JobUtilization) =
    j.getJobInfo.getId -> j
  
  def keyByTask(t: TaskUtilization): ((Long, Int), TaskUtilization) =
    (t.getInfo.getJob.getId -> t.getInfo.getTaskIndex, t)

  def keyByTask(t: TaskUsage): ((Long, Int), TaskUsage) =
    (t.getTaskInfo.getJob.getId -> t.getTaskInfo.getTaskIndex, t)

  def combineUsage(usage: RDD[TaskUsage], 
                   maybeTasks: Option[RDD[TaskUtilization]],
                   maybeJobs: Option[RDD[JobUtilization]]): RDD[TaskUsageWithAvg] = {
    val usageEncap = usage.map(u => TaskUsageWithAvg.newBuilder.setUsage(u).build)
    val usageWithTasks = maybeTasks match {
    case Some(tasks) => {
      val tasksMap = usage.context.broadcast(tasks.map(keyByTask).collect.toMap)
      usageEncap.map(u => {
        val taskUtil = tasksMap.value.get(u.getUsage.getTaskInfo.getJob.getId ->
                                          u.getUsage.getTaskInfo.getTaskIndex)
        val builder = TaskUsageWithAvg.newBuilder.mergeFrom(u)
        taskUtil.foreach(builder.setTask _)
        builder.build
      })
    }
    case None => usageEncap
    }
    val usageWithJobs = maybeJobs match {
    case Some(jobs) => {
      val jobsMap = usage.context.broadcast(jobs.map(keyByJob).collect.toMap)
      usageWithTasks.map(u => {
        val jobUtil = jobsMap.value.get(u.getUsage.getTaskInfo.getJob.getId)
        val builder = TaskUsageWithAvg.newBuilder.mergeFrom(u)
        jobUtil.foreach(builder.setJob _)
        builder.build
      })
    }
    case None => usageWithTasks
    }

    usageWithJobs
  }

  def keyUsageByMT(usage: TaskUsageWithAvg): ((Long, Long), TaskUsageWithAvg) = 
    (usage.getUsage.getMachineInfo.getId,
     normalizeTime(usage.getUsage.getStartTime)) -> usage

  def accumulateUsage(usage: Seq[TaskUsage]): Resources = {
    def usageKey(u: TaskUsage): (Long, Int) =
      (u.getTaskInfo.getJob.getId, u.getTaskInfo.getTaskIndex)
    /* TODO: fix the ordering here */
    val usageByTask = scala.collection.immutable.Map[(Long, Int), TaskUsage](
      usage.map(u => usageKey(u) -> u): _*
    )
    def weight(u: TaskUsage): Double =
      (u.getEndTime - u.getStartTime) / (300.0 * 1000.0 * 1000.0)
    var cpu = usage.map(u => u.getResources.getCpus * weight(u)).sum
    var mem = usageByTask.values.map(u => u.getResources.getMemory).sum
    Resources.newBuilder.setCpus(cpu.asInstanceOf[Float]).setMemory(mem.asInstanceOf[Float]).build
  }

  def toUsageByMachine(usage: Seq[TaskUsageWithAvg]): UsageByMachine = {
    import scala.collection.JavaConversions._
    val startTime = usage.map(_.getUsage.getStartTime).min
    val endTime = usage.map(_.getUsage.getEndTime).max
    val info = usage.head.getUsage.getMachineInfo
    val totalUsage = accumulateUsage(usage.map(_.getUsage))
    val result = UsageByMachine.newBuilder
    result.setResources(accumulateUsage(usage.map(_.getUsage))).
           addAllComponentsWithAvg(usage).
           setStartTime(startTime).
           setInfo(info).
           setEndTime(endTime).build
  }

  def computeUsageByMachine(usage: RDD[TaskUsageWithAvg]): RDD[UsageByMachine] = {
    usage.map(keyUsageByMT).groupByKey.mapValues(toUsageByMachine).
          map(kv => kv._2)
  }
  
  def computeUsageByMachineU(usage: RDD[TaskUsage]): RDD[UsageByMachine] = {
    computeUsageByMachine(combineUsage(usage, None, None))
  }

  def writeUsageByMachine(sc: SparkContext, rate: Int, data: RDD[UsageByMachine]): Unit = {
    import Stored._
    out[LzoUsageByMachineProtobufBlockOutputFormat, UsageByMachine](sc, data,
      outDir + "/sample" + rate + "_usage_by_machine")
  }

  def findComponents(u: UsageByMachine): Seq[TaskUsage] = {
    import scala.collection.JavaConversions._
    if (u.getComponentsCount > 0)
      u.getComponentsList
    else
      u.getComponentsWithAvgList.map(_.getUsage)
  }

  def getUsages(u: UsageByMachine, f: Resources => Float): (Float, Float, Float, Float, Float) = {
    import scala.collection.JavaConversions._
    val used = f(u.getResources)
    val capacity = f(u.getInfo.getCapacity)
    val uniqueComponents = findComponents(u).map(x => 
      (x.getTaskInfo.getJob.getId, x.getTaskInfo.getTaskIndex) -> x).toMap.values
    val reserved =
      uniqueComponents.map(x => f(x.getTaskInfo.getRequestedResources)).sum
    val reservedHigh =
      uniqueComponents.filter(_.getTaskInfo.getPriority > 8).map(
        x => f(x.getTaskInfo.getRequestedResources)).sum
    val usedHigh =
      uniqueComponents.filter(_.getTaskInfo.getPriority > 8).map(
        x => f(x.getResources)).sum
    (used, capacity, reserved, reservedHigh, usedHigh)
  }

  def getUsagesString(x: (Float, Float, Float, Float, Float)) =
    "used %s capacity %s reserved %s reservedHigh %s usedHigh %s".format(
      x._1, x._2, x._3, x._4, x._5)
}
