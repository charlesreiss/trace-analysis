package edu.berkeley.cs.amplab

import spark.SparkContext
import spark.RDD
import SparkContext._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptID

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable
import com.twitter.elephantbird.util.TypeRef

import edu.berkeley.cs.amplab.mapreduce.output._
import edu.berkeley.cs.amplab.GoogleTrace._

import com.google.protobuf.Message
import com.google.protobuf.Descriptors.Descriptor

import spark.SerializableWritable
import java.text.SimpleDateFormat
import java.util.Date

object ConvertTaskUsage {

  def writeHadoop[F <: OutputFormat[K, V], K, V](data: RDD[(K, V)], path: String)(
      implicit fm: ClassManifest[F]): Int = {
    val job = new Job
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    FileOutputFormat.setOutputPath(job, new Path(path))
    /* XXX from HadoopFileWriter*/
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = data.id
    def doWrite(context: spark.TaskContext, iter: Iterator[(K,V)]): Int = {
      val attemptId = new TaskAttemptID(jobtrackerID,
        stageId, false, context.splitId, context.attemptId)
      val hadoopContext= new TaskAttemptContext(wrappedConf.value, attemptId)
      val format = fm.erasure.newInstance.asInstanceOf[F]
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)
      val writer = format.getRecordWriter(hadoopContext)
      while (iter.hasNext) {
        val (k, v) = iter.next
        writer.write(k, v)
      }
      writer.close(hadoopContext)
      committer.commitTask(hadoopContext)
      return 1
    }
    val jobFormat = fm.erasure.newInstance.asInstanceOf[F]
    val jobAttemptId = new TaskAttemptID(jobtrackerID, stageId, true, 0, 0)
    val jobTaskContext = new TaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    val count = data.context.runJob(data, doWrite _).sum
    jobCommitter.commitJob(jobTaskContext)
    return count
  }

  def convertTime(s: String): java.lang.Long = {
    if (s == "18446744073709551615") {
      return Long.MaxValue
    } else {
      return java.lang.Long.parseLong(s)
    }
  }
  
  def convertId(s: String): java.lang.Long =
    java.lang.Long.parseLong(s)
  def convertInt(s: String): java.lang.Integer =
    java.lang.Integer.parseInt(s)
  def convertFloat(s: String): java.lang.Float =
    java.lang.Float.parseFloat(s)
  def convertBool(s: String): java.lang.Boolean = s == "1"

  def makeSetter(messageDesc: Descriptor, field: String,
                 converter: String => AnyRef):
      (Message.Builder, String) => Message.Builder = {
    val fieldDesc = messageDesc.findFieldByName(field)
    def setter(builder: Message.Builder, value: String): Message.Builder = 
      if (value != "")
        builder.setField(fieldDesc, converter(value))
      else
        builder
    return setter
  }

  
  def convertRecord(values: Array[String]): TaskUsage = {
    val builder = TaskUsage.newBuilder
    val resourcesBuilder = builder.getResourcesBuilder
    val maxResourcesBuilder = builder.getMaxResourcesBuilder
    def field(i: Int, name: String, converter: String => AnyRef): Unit = 
      makeSetter(TaskUsage.getDescriptor, name, converter)(builder, values(i))
    def field2(builder: Message.Builder)(
        i: Int, name: String, converter: String => AnyRef): Unit =
      makeSetter(builder.getDescriptorForType, name, converter)(
                 builder, values(i))
    val resourcesField = field2(resourcesBuilder) _
    val maxResourcesField = field2(maxResourcesBuilder) _
    field(0, "start_time", convertTime)
    field(1, "end_time", convertTime)
    field(2, "job_id", convertId)
    field(3, "task_index", convertInt)
    field(4, "machine_id", convertId)
    resourcesField(5, "cpus", convertFloat)
    resourcesField(6, "memory", convertFloat)
    resourcesField(7, "assigned_memory", convertFloat)
    resourcesField(8, "unmapped_page_cache_memory", convertFloat)
    resourcesField(9, "page_cache_memory", convertFloat)
    maxResourcesField(10, "memory", convertFloat)
    resourcesField(11, "disk_time", convertFloat)
    resourcesField(12, "disk_space", convertFloat)
    maxResourcesField(13, "cpus", convertFloat)
    maxResourcesField(14, "disk_time", convertFloat)
    field(15, "cycles_per_instruction", convertFloat)
    field(16, "memory_accesses_per_instruction", convertFloat)
    field(17, "sample_portion", convertFloat)
    field(18, "aggregation_type", convertBool)
    return builder.build
  }

  def fixupForOutput[T <: Message](x: T): (T, ProtobufWritable[T]) = 
    return (null.asInstanceOf[T], new ProtobufWritable[T](x, new TypeRef[T] {}))

  def write[F <: OutputFormat[T, ProtobufWritable[T]], T <: Message](
      path: String, data: RDD[T])(implicit fm: ClassManifest[F]): Unit = {
    val fixedData = data.map(fixupForOutput)
    writeHadoop[F, T, ProtobufWritable[T]](fixedData, path)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(args(0), "convert-task-usage")
    val input: RDD[(LongWritable, Text)] = sc.hadoopFile(args(1))

    val lines: RDD[Array[String]] = input.map(kv => kv._2.toString.split(","))

    val records = lines.map(convertRecord)

    val outputName = args(2)

    write[LzoTaskUsageProtobufBlockOutputFormat, TaskUsage](outputName,
                                                            records)
  }
}
