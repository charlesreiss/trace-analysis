package edu.berkeley.cs.amplab

import scala.collection.mutable.Buffer

import spark.SparkContext
import spark.RDD
import SparkContext._

import Util._
import GoogleTrace._

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
import com.google.protobuf.{CodedInputStream, CodedOutputStream}

import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import com.esotericsoftware.kryo.serialize.ArraySerializer
import com.esotericsoftware.kryo.compress.DeflateCompressor
import java.nio.ByteBuffer

class MyKryoRegistrator extends spark.KryoRegistrator {
  def registerClasses(kyro: Kryo): Unit = {
    MyKryoRegistrator.realRegisterClasses(kyro)
  }
}

object MyKryoRegistrator {
  val tlBuffer = new java.lang.ThreadLocal[Array[Byte]] {
    override def initialValue: Array[Byte] = new Array[Byte](1024 * 128)
  }
  abstract class PBSerialize[T <: Message] extends KSerializer {
    override def writeObjectData(buf: ByteBuffer, _obj: AnyRef) {
      val obj = _obj.asInstanceOf[T]
      val tempBuf = tlBuffer.get
      obj.writeTo(CodedOutputStream.newInstance(tempBuf))
      val len = obj.getSerializedSize
      buf.putInt(obj.getSerializedSize)
      buf.put(tempBuf, 0, len)
    }
    def parseFrom(in: CodedInputStream): T
    override def readObjectData[U](buf: ByteBuffer, cls: Class[U]): U = {
      val len = buf.getInt
      val tempBuf = tlBuffer.get
      buf.get(tempBuf, 0, len)
      parseFrom(CodedInputStream.newInstance(tempBuf, 0, len)).asInstanceOf[U]
    }
  }
  def realRegisterClasses(kyro: Kryo): Unit = {
    kyro.register(classOf[TaskUsage], new PBSerialize[TaskUsage] {
      final override def parseFrom(in: CodedInputStream) = TaskUsage.parseFrom(in)
    })
    kyro.register(classOf[TaskEvent], new PBSerialize[TaskEvent] {
      final override def parseFrom(in: CodedInputStream) = TaskEvent.parseFrom(in)
    })
    kyro.register(classOf[JobEvent], new PBSerialize[JobEvent] {
      final override def parseFrom(in: CodedInputStream) = JobEvent.parseFrom(in)
    })
    kyro.register(classOf[MachineEvent], new PBSerialize[MachineEvent] {
      final override def parseFrom(in: CodedInputStream) =
          MachineEvent.parseFrom(in)
    })
    kyro.register(classOf[UsageByMachine], new PBSerialize[UsageByMachine] {
      final override def parseFrom(in: CodedInputStream) =
          UsageByMachine.parseFrom(in)
    })
    for (cls <- List(classOf[Array[TaskUsage]], classOf[Array[TaskEvent]],
                     classOf[Array[JobEvent]], classOf[Array[MachineEvent]])) {
      kyro.register(cls, new DeflateCompressor(new ArraySerializer(kyro)))
    }
  }
}

object Convert {
  def convertTaskUsage(v: Array[String]): TaskUsage = {
    val builder = TaskUsage.newBuilder
    val taskInfoBuilder = builder.getTaskInfoBuilder
    val jobInfoBuilder = taskInfoBuilder.getJobBuilder
    val machineInfoBuilder = builder.getMachineInfoBuilder
    val resourcesBuilder = builder.getResourcesBuilder
    val maxResourcesBuilder = builder.getMaxResourcesBuilder
    builder.setStartTime(convertTime(v(0)))
    builder.setEndTime(convertTime(v(1)))
    jobInfoBuilder.setId(convertId(v(2)))
    taskInfoBuilder.setTaskIndex(convertInt(v(3)))
    machineInfoBuilder.setId(convertId(v(4)))
    if (!(v(5) isEmpty)) resourcesBuilder.setCpus(convertFloat(v(5)))
    if (!(v(6) isEmpty)) resourcesBuilder.setMemory(convertFloat(v(6)))
    if (!(v(7) isEmpty)) resourcesBuilder.setAssignedMemory(convertFloat(v(7)))
    if (!(v(8) isEmpty))
      resourcesBuilder.setUnmappedPageCacheMemory(convertFloat(v(8)))
    if (!(v(9) isEmpty)) resourcesBuilder.setPageCacheMemory(convertFloat(v(9)))
    if (!(v(10) isEmpty)) maxResourcesBuilder.setMemory(convertFloat(v(10)))
    if (!(v(11) isEmpty)) resourcesBuilder.setDiskTime(convertFloat(v(11)))
    if (!(v(12) isEmpty)) resourcesBuilder.setDiskSpace(convertFloat(v(12)))
    if (!(v(13) isEmpty)) maxResourcesBuilder.setCpus(convertFloat(v(13)))
    if (!(v(14) isEmpty)) maxResourcesBuilder.setDiskTime(convertFloat(v(14)))
    if (!(v(15) isEmpty)) builder.setCyclesPerInstruction(convertFloat(v(15)))
    if (!(v(16) isEmpty))
      builder.setMemoryAccessesPerInstruction(convertFloat(v(16)))
    if (!(v(17) isEmpty)) builder.setSamplePortion(convertFloat(v(17)))
    if (!(v(18) isEmpty)) builder.setAggregationType(convertBool(v(18)))
    return builder.build
  }

  def convertTaskEvent(v: Array[String]): TaskEvent = {
    val builder = TaskEvent.newBuilder
    val taskInfoBuilder = builder.getInfoBuilder
    val resourcesBuilder = taskInfoBuilder.getRequestedResourcesBuilder
    val jobInfoBuilder = taskInfoBuilder.getJobBuilder
    val machineInfoBuilder = builder.getMachineInfoBuilder
    builder.setTime(convertTime(v(0)))
    if (!(v(1) isEmpty)) builder.setMissingType(convertMissingType(v(1)))
    jobInfoBuilder.setId(convertId(v(2)))
    taskInfoBuilder.setTaskIndex(convertInt(v(3)))
    if (!(v(4) isEmpty)) {
      machineInfoBuilder.setId(convertId(v(4)))
    } else {
      builder.clearMachineInfo
    }
    builder.setType(TaskEventType.valueOf(convertInt(v(5))))
    if (!(v(6) isEmpty)) jobInfoBuilder.setUser(v(6))
    if (!(v(7) isEmpty)) taskInfoBuilder.setSchedulingClass(convertInt(v(7)))
    if (!(v(8) isEmpty)) taskInfoBuilder.setPriority(convertInt(v(8)))
    if (!(v(9) isEmpty)) resourcesBuilder.setCpus(convertFloat(v(9)))
    if (!(v(10) isEmpty)) resourcesBuilder.setMemory(convertFloat(v(10)))
    if (!(v(11) isEmpty)) resourcesBuilder.setDiskSpace(convertFloat(v(11)))
    if (!(v(12) isEmpty)) taskInfoBuilder.setDifferentMachines(convertBool(v(12)))
    return builder.build
  }

  def convertJobEvent(v: Array[String]): JobEvent = {
    val builder = JobEvent.newBuilder
    val jobInfoBuilder = builder.getInfoBuilder

    builder.setTime(convertTime(v(0)))
    if (!(v(1) isEmpty)) builder.setMissingType(convertMissingType(v(1)))
    jobInfoBuilder.setId(convertId(v(2)))
    builder.setType(TaskEventType.valueOf(convertInt(v(3))))
    if (!(v(4) isEmpty)) jobInfoBuilder.setUser(v(4))
    if (!(v(5) isEmpty)) jobInfoBuilder.setSchedulingClass(convertInt(v(5)))
    if (!(v(6) isEmpty)) jobInfoBuilder.setName(v(6))
    if (!(v(7) isEmpty)) jobInfoBuilder.setLogicalName(v(7))
    return builder.build
  }

  def convertMachineEvent(v: Array[String]): MachineEvent = {
    val builder = MachineEvent.newBuilder
    val machineInfoBuilder = builder.getInfoBuilder
    val capacityBuilder = machineInfoBuilder.getCapacityBuilder

    builder.setTime(convertTime(v(0)))
    machineInfoBuilder.setId(convertId(v(1)))
    val raw_type = convertInt(v(2))
    if (raw_type == 0 /* ADD */ || raw_type == 2 /* UPDATE */) {
      builder.setUp(true)
    } else {
      builder.setUp(false)
    }
    if (!(v(3) isEmpty)) machineInfoBuilder.setPlatformId(v(3))
    if (!(v(4) isEmpty)) capacityBuilder.setCpus(convertFloat(v(4)))
    if (!(v(5) isEmpty)) capacityBuilder.setMemory(convertFloat(v(5)))
    return builder.build
  }

  val COMMA = java.util.regex.Pattern.compile(",")
    
  trait TimeOf[T] extends Function1[T, Long] with Serializable {
    def apply(value: T): Long
    def lessThan(first: T, second: T): Boolean = apply(first) < apply(second)
  }

  implicit def taskEventTime: TimeOf[TaskEvent] = new TimeOf[TaskEvent] {
    def apply(value: TaskEvent): Long = 
      if (value.getType == TaskEventType.SCHEDULE)
        value.getTime + 1
      else
        value.getTime
  }
  implicit def jobEventTime: TimeOf[JobEvent] = new TimeOf[JobEvent] {
    def apply(value: JobEvent): Long =
      if (value.getType == TaskEventType.SCHEDULE)
        value.getTime + 1
      else
        value.getTime
  }
  implicit def machineEventTime: TimeOf[MachineEvent] = new TimeOf[MachineEvent]
  {
    def apply(value: MachineEvent): Long = value.getTime
  }

  implicit def taskUsageTime: TimeOf[TaskUsage] = new TimeOf[TaskUsage]
  {
    def apply(value: TaskUsage): Long = value.getStartTime + 1000000L
  }

  trait Insert[T, U, K] extends Serializable {
    def throughT(t: T): Boolean = false
    def hasThroughT: Boolean = false
    def apply(into: T, value: U): T
    def keyT(t: T): K
    def keyU(u: U): K
  }

  implicit def insertJobInTask: Insert[TaskEvent, JobEvent, Long] =
    new Insert[TaskEvent, JobEvent, Long] {
      def apply(into: TaskEvent, value: JobEvent): TaskEvent = {
        val builder = into.toBuilder
        builder.getInfoBuilder.getJobBuilder.mergeFrom(value.getInfo)
        return builder.build
      }
      def keyT(t: TaskEvent) = t.getInfo.getJob.getId
      def keyU(u: JobEvent) = u.getInfo.getId
    }

  implicit def insertMachineInTask: Insert[TaskEvent, MachineEvent, Long] =
    new Insert[TaskEvent, MachineEvent, Long] {
      override def hasThroughT: Boolean = true
      override def throughT(t: TaskEvent): Boolean = 
        !t.hasMachineInfo || !t.getMachineInfo.hasId ||
        t.getMachineInfo.getId == 0
      def apply(into: TaskEvent, value: MachineEvent): TaskEvent = {
        val builder = into.toBuilder
        builder.getMachineInfoBuilder.mergeFrom(value.getInfo)
        return builder.build
      }
      def keyT(t: TaskEvent) = 
        if (t.hasMachineInfo && t.getMachineInfo.hasId) 
          t.getMachineInfo.getId
        else
          -1L
      def keyU(u: MachineEvent) = u.getInfo.getId
    }

  implicit def insertTaskInUsage: Insert[TaskUsage, TaskEvent, (Long, Int)] =
    new Insert[TaskUsage, TaskEvent, (Long, Int)] {
      def apply(into: TaskUsage, value: TaskEvent): TaskUsage = {
        val builder = into.toBuilder
        builder.getTaskInfoBuilder.mergeFrom(value.getInfo)
        return builder.build
      }
      def keyT(t: TaskUsage) = (t.getTaskInfo.getJob.getId,
                                t.getTaskInfo.getTaskIndex)
      def keyU(t: TaskEvent) = (t.getInfo.getJob.getId,
                                t.getInfo.getTaskIndex)
    }

  implicit def insertMachineInUsage: Insert[TaskUsage, MachineEvent, Long] =
    new Insert[TaskUsage, MachineEvent, Long] {
      def apply(into: TaskUsage, value: MachineEvent): TaskUsage = {
        val builder = into.toBuilder
        builder.getMachineInfoBuilder.mergeFrom(value.getInfo)
        return builder.build
      }
      def keyT(t: TaskUsage) = t.getMachineInfo.getId
      def keyU(u: MachineEvent) = u.getInfo.getId
    }

  val TIME_PERIOD = 1000L * 1000L * 60L * 60L
  val MAX_TIME = 1000L * 1000L * 60L * 60L * 24L * 30L

  /*
  def placeJoinedBig[@specialized RealKey, T, U <: Message](into: RDD[T], values: RDD[U])
      (implicit timeOfT: TimeOf[T], timeOfU: TimeOf[U],
       insertU: Insert[T,U,RealKey],
       km: ClassManifest[RealKey], tm: ClassManifest[T], um: ClassManifest[U])
      : RDD[T] = {
    type K = (Int, RealKey)
    val keyedInto: RDD[(K, T)] =
      keyByTime(into, insertU.keyT _, timeOfT, TIME_PERIOD, MAX_TIME)
    val keyedValues: RDD[(K, U)] = 
      partitionByTime(values, insertU.keyU _, timeOfU, timeOfU.lessThan _,
                      TIME_PERIOD, MAX_TIME)
    val grouped: RDD[(K, (Seq[T], Seq[U]))] = keyedInto.groupWith(keyedValues)
    def processGroup(kv: (K, (Seq[T], Seq[U]))): Seq[T] = {
      val v = kv._2
      val sortedT: Seq[T] = v._1.sortBy(x => timeOfT(x))
      val sortedU: Seq[U] = v._2.sortBy(x => timeOfU(x))
      val result = Buffer.empty[T]
      val uIterator = sortedU.iterator.buffered
      var currentU: Option[U] = None
      for (t <- sortedT) {
        val currentTime = timeOfT(t)
        while (uIterator.hasNext && currentTime >= timeOfU(uIterator.head)) {
          currentU = Some(uIterator.next)
        }
        currentU match {
        case Some(u) => result += insertU(t, u)
        case None => result += t
        }
      }
      return result
    }
    return grouped.flatMap(processGroup)
  }
  */

  def placeJoined[@specialized RealKey, T, U](_into: RDD[T], values: RDD[U])
      (implicit timeOfT: TimeOf[T], timeOfU: TimeOf[U],
       insertU: Insert[T,U,RealKey],
       km: ClassManifest[RealKey], tm: ClassManifest[T], um: ClassManifest[U])
      : RDD[T] = {
    type K = RealKey
    val throughInto: RDD[T] =
      if (insertU.hasThroughT)
        _into.filter(t => insertU.throughT(t))
      else
        _into.context.makeRDD(Array[T]())
    val into: RDD[T] =
      if (insertU.hasThroughT)
        _into.filter(t => !insertU.throughT(t))
      else
        _into
    val keyedInto: RDD[(K, T)] = into.map(t => insertU.keyT(t) -> t)
    val keyedValues: RDD[(K, U)] = values.map(u => insertU.keyU(u) -> u)
    val grouped: RDD[(K, (Seq[T], Seq[U]))] =
        keyedInto.groupWith(keyedValues)
    def processGroup(kv: (K, (Seq[T], Seq[U]))): Seq[T] = {
      val v = kv._2
      val sortedT: Seq[T] = v._1.sortBy(x => timeOfT(x))
      val sortedU: Seq[U] = v._2.sortBy(x => timeOfU(x))
      val result = Buffer.empty[T]
      val uIterator = sortedU.iterator.buffered
      var currentU: Option[U] = None
      for (t <- sortedT) {
        val currentTime = timeOfT(t)
        while (uIterator.hasNext && currentTime >= timeOfU(uIterator.head)) {
          currentU = Some(uIterator.next)
        }
        currentU match {
        case Some(u) => result += insertU(t, u)
        case None => result += t
        }
      }
      return result
    }
    val afterProcess = grouped.flatMap(processGroup)
    if (insertU.hasThroughT)
      afterProcess ++ throughInto
    else
      afterProcess
  }

  def reshardStrings(data: RDD[String]): RDD[String] =
    data.map(x => (x, Nil)).groupByKey(2000).map(_._1)

  def in[T <: Message](sc: SparkContext, convert: Array[String] => T,
                       inDir: String, isBig: Boolean = true)(implicit fm: ClassManifest[T]): RDD[T] = {
    val _lines = sc.textFile(inDir + "/*?????-of-?????.csv*")
    val lines = if (isBig) reshardStrings(_lines) else _lines
    val records = lines.map(COMMA.split(_, -1))
    return records.map(convert)
  }

  def inLzo[K, V, F <: InputFormat[K, V]](sc: SparkContext, path: String)(
      implicit fm: ClassManifest[F], km: ClassManifest[K],
      vm: ClassManifest[V]): RDD[(K, V)] = {
    val conf = new Configuration
    conf.set("io.compression.codecs",
      "org.apache.hadoop.io.compress.DefaultCodec," +
      "org.apache.hadoop.io.compress.GzipCodec," +
      "org.apache.hadoop.io.compress.BZip2Codec," +
      "com.hadoop.compression.lzo.LzoCodec," +
      "com.hadoop.compression.lzo.LzopCodec")
    conf.set("io.compression.codec.lzo.class", 
      "com.hadoop.compression.lzo.LzoCodec")
    val job = new HadoopJob(conf)
    FileInputFormat.addInputPath(job, new Path(path))
    sc.newAPIHadoopFile(path,
      fm.erasure.asInstanceOf[Class[F]],
      km.erasure.asInstanceOf[Class[K]],
      vm.erasure.asInstanceOf[Class[V]],
      job.getConfiguration)
  }

  def inConverted[F <: InputFormat[LongWritable, ProtobufWritable[T]],
                  T <: Message](sc: SparkContext, path: String)(
                  implicit fm: ClassManifest[F], tm: ClassManifest[T]): RDD[T] =
    inLzo[LongWritable, ProtobufWritable[T], F](sc, path).map(_._2.get)
  
  def out[F <: OutputFormat[T, ProtobufWritable[T]], T <: Message](
      sc: SparkContext,
      data: RDD[T],
      out: String)(implicit fm: ClassManifest[F], tm: ClassManifest[T]): Unit = {
    write[F, T](out, data)
  } 

  def keyByTask(taskEvent: TaskEvent): ((Long, Int), TaskEvent) =
    (taskEvent.getInfo.getJob.getId, taskEvent.getInfo.getTaskIndex) -> taskEvent
  
  def keyByJob(taskEvent: TaskEvent): (Long, TaskEvent) =
    taskEvent.getInfo.getJob.getId -> taskEvent

  var inDir = "/work/charles/clustertrace3"
  var outSan = "/work/charles/clustertrace-resharded"
  def putTasks(sc: SparkContext, data: RDD[TaskEvent], outFile: String): Unit = {
    out[LzoTaskEventProtobufBlockOutputFormat, TaskEvent](sc, data, outFile)
  }
  
  def putUsage(sc: SparkContext, data: RDD[TaskUsage], outFile: String): Unit = {
    out[LzoTaskUsageProtobufBlockOutputFormat, TaskUsage](sc, data, outFile)
  }
  
  def putMachines(sc: SparkContext, data: RDD[MachineEvent], outFile: String): Unit = {
    out[LzoMachineEventProtobufBlockOutputFormat, MachineEvent](sc, data, outFile)
  }

  def origGetTasks(sc: SparkContext): RDD[TaskEvent] =
    in(sc, convertTaskEvent, inDir + "/task_events", true)

  def getTasks(sc: SparkContext): RDD[TaskEvent] =
    inConverted[LzoTaskEventProtobufBlockInputFormat, TaskEvent](
      sc, outSan + "/tasks_and_jobs/*.lzo")

  def getUsage(sc: SparkContext): RDD[TaskUsage] = 
    inConverted[LzoTaskUsageProtobufBlockInputFormat, TaskUsage](
      sc, outSan + "/usage")

  def origGetUsage(sc: SparkContext): RDD[TaskUsage] = 
    in(sc, convertTaskUsage, inDir + "/task_usage", true).cache

  def runConvert(sc: SparkContext, inDir: String, outDir: String): Unit = {
    val rawTasks = in(sc, convertTaskEvent, inDir + "/task_events").cache
    val rawJobs = in(sc, convertJobEvent, inDir + "/job_events").cache
    val rawMachines = in(sc, convertMachineEvent, inDir + "/machine_events").cache
    val rawTaskUsage = in(sc, convertTaskUsage, inDir + "/task_usage").cache
    val tasksPlusJobs = placeJoined(rawTasks, rawJobs)
    /* val tasksPlusMachines = placeJoined(tasksPlusJobs, rawMachines).cache
    val usagePlusTasks = placeJoined(rawTaskUsage, tasksPlusMachines)
    val usagePlusMachines = placeJoined(usagePlusTasks, rawMachines) 
    out[LzoJobEventProtobufBlockOutputFormat, JobEvent](sc,
        rawJobs, outDir + "/job_events") */
    out[LzoTaskEventProtobufBlockOutputFormat, TaskEvent](sc, tasksPlusJobs,
                                              outDir + "/task_events")
    /* out[LzoTaskEventProtobufBlockOutputFormat, TaskEvent](sc, tasksPlusMachines,
                                              outDir + "/task_events") */
    /*
    out[LzoTaskUsageProtobufBlockOutputFormat, TaskUsage](sc, usagePlusMachines,
                                               outDir + "/task_usage")
    out[LzoMachineEventProtobufBlockOutputFormat, MachineEvent](sc, rawMachines,
                                                  outDir + "/machine_events")
    */
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(args(0), "convert-trace")
    runConvert(sc, args(1), args(2))
    System.exit(0)
  }
}
