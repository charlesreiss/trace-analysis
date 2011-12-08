package amplab.googletrace

import Convert._
import Protos._

import amplab.googletrace.mapreduce.input._
import amplab.googletrace.mapreduce.output._
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable
import org.apache.hadoop.io.LongWritable

import spark.SparkContext
import spark.RDD
import SparkContext._

object Stored {
  var inDir = "/work/charles/clustertrace3"
  var outSan = "/work/charles/clustertrace-resharded"

  def readSavedTasks(sc: SparkContext, inFile: String): RDD[TaskEvent] = 
    inLzo[LongWritable, ProtobufWritable[TaskEvent],
          LzoTaskEventProtobufBlockInputFormat](sc, inFile).
    map(kv => kv._2.get)

  def readSavedUsage(sc: SparkContext, inFile: String): RDD[TaskUsage] = 
    inLzo[LongWritable, ProtobufWritable[TaskUsage],
          LzoTaskUsageProtobufBlockInputFormat](sc, inFile).
    map(kv => kv._2.get)

  def readSavedMachines(sc: SparkContext, inFile: String): RDD[MachineEvent] = 
    inLzo[LongWritable, ProtobufWritable[MachineEvent],
          LzoMachineEventProtobufBlockInputFormat](sc, inFile).
    map(kv => kv._2.get)

  def getTasks(sc: SparkContext): RDD[TaskEvent] = 
    readSavedTasks(sc, outSan + "/all_tasks")
  def getUsage(sc: SparkContext): RDD[TaskUsage] =
    readSavedUsage(sc, outSan + "/all_usage_w_m")
  def getMachines(sc: SparkContext): RDD[MachineEvent] =
    readSavedMachines(sc, outSan + "/machine_events")
  def getJobs(sc: SparkContext): RDD[JobEvent] =
    in(sc, convertJobEvent, inDir + "/job_events", false)
}
