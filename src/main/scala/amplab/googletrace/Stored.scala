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
  var inDir = System.getProperty("trace.in.directory")
  var outDir = System.getProperty("trace.processed.directory")

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

  def readSavedJobs(sc: SparkContext, inFile: String): RDD[JobEvent] = 
    inLzo[LongWritable, ProtobufWritable[JobEvent],
          LzoJobEventProtobufBlockInputFormat](sc, inFile).
    map(kv => kv._2.get)

  def getTasks(sc: SparkContext): RDD[TaskEvent] = 
    readSavedTasks(sc, outDir + "/all_tasks_marked")
  def getUsage(sc: SparkContext): RDD[TaskUsage] =
    readSavedUsage(sc, outDir + "/all_usage_w_m")
  def getMachines(sc: SparkContext): RDD[MachineEvent] =
    readSavedMachines(sc, outDir + "/machine_events")
  def getJobs(sc: SparkContext): RDD[JobEvent] =
    in(sc, convertJobEvent, inDir + "/job_events", false)

  def putJobs(sc: SparkContext, data: RDD[JobEvent], outFile: String): Unit = {
    out[LzoJobEventProtobufBlockOutputFormat, JobEvent](sc, data, outFile)
  }
  
  def putTasks(sc: SparkContext, data: RDD[TaskEvent], outFile: String): Unit = {
    out[LzoTaskEventProtobufBlockOutputFormat, TaskEvent](sc, data, outFile)
  }
  
  def putUsage(sc: SparkContext, data: RDD[TaskUsage], outFile: String): Unit = {
    out[LzoTaskUsageProtobufBlockOutputFormat, TaskUsage](sc, data, outFile)
  }
  
  def putMachines(sc: SparkContext, data: RDD[MachineEvent], outFile: String): Unit = {
    out[LzoMachineEventProtobufBlockOutputFormat, MachineEvent](sc, data, outFile)
  }
}
