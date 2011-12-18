package amplab.googletrace

import java.io.{DataOutput, DataOutputStream, FileOutputStream}

object ToNumpy {
  trait Converter[T] {
    def writeNumpy(path: String, data: Array[T]): Unit
  }

  def converter[T](
    getValues: T => (Array[String], Array[Float]),
    fieldNames: (Array[String], Array[String])
  ): Converter[T] = {
    return new Converter[T] {
      val namesString =
          "[%s]".format(
            (fieldNames._1 ++ fieldNames._2).map("'" + _ + "'").mkString(",")
          )
      val typesString = 
          (List.fill(fieldNames._1.size)("'S40'") ++
           List.fill(fieldNames._2.size)("'>f4'")).mkString(",")
      val dtypeString =
          "{'names':%s,'formats':[%s]}".format(namesString, typesString)
      def writeLine(t: T, out: DataOutput): Unit = {
        val (strings, floats) = getValues(t)
        assert(strings.size == fieldNames._1.size)
        assert(floats.size == fieldNames._2.size)
        for (s <- strings) {
          val padded =
            (s + "                                            ").substring(0, 40)
          out.writeBytes(padded)
        }
        for (f <- floats) {
          out.writeFloat(f)
        }
      }
          
      override def writeNumpy(path: String, data: Array[T]): Unit = {
        val metaString = 
            "{'descr':%s,'fortran_order':False,'shape':(%d,)}\n".format(
              dtypeString, data.size
            )
        val out = new DataOutputStream(new FileOutputStream(path))
        out.writeBytes("\u0093NUMPY\u0001\u0000")
        val metaLen = metaString.size
        out.writeByte(metaLen % 256)
        out.writeByte(metaLen / 256)
        out.writeBytes(metaString)
        data.foreach(writeLine(_, out))
        out.close
      }
    }
  }

  import TraceUtil._
  lazy val jobUtilizationConverter: Converter[JobUtilization] = {
    def getValues(t: JobUtilization): (Array[String], Array[Float]) = {
      import scala.collection.JavaConversions._
      (
        Array[String](
          t.getJobInfo.getName,
          t.getJobInfo.getLogicalName,
          t.getJobInfo.getUser
        ),
        Array[Float](
          t.getJobInfo.getSchedulingClass,
          t.getTaskSamples(0).getPriority,
          if (t.getTaskSamples(0).getDifferentMachines)
            0f
          else
            1f,
          t.getStartTime,
          t.getEndTime,
          t.getEndTime - t.getStartTime,
          t.getRunningTime,
          t.getNumTasks,
          t.getMinRequest.getCpus,
          t.getMinRequest.getMemory,
          t.getMaxRequest.getCpus,
          t.getMaxRequest.getMemory 
        ) ++ (0 to 11).flatMap(
          i => List(t.getPercentileTaskUsage(i).getCpus,
                    t.getPercentileTaskUsage(i).getMemory,
                    t.getPercentileMeanTaskUsage(i).getCpus,
                    t.getPercentileMeanTaskUsage(i).getMemory)
        ) ++ (0 to 8).map(
          i => t.getTaskSamplesList.map(_.getNumEventsByType(i)).sum.floatValue
        ) ++ List(
          t.getTaskSamplesList.map(_.getNumMissing).sum.floatValue,
          t.getTaskSamplesList.map(_.getNumEvents).sum.floatValue
        )
      )
    }
    val stringNames = Array[String](
        "name", "logical_name", "user"
    )
    val floatNames = Array[String](
        "scheduling_class", "priority", "different_machines",
        "start_time", "end_time", "duration", "running_time",
        "num_tasks",
        "min_req_cpus", "min_req_memory",
        "max_req_cpus", "max_req_memory" 
    ) ++ List("t50_pt50", "t50_pt90", "t50_pt99", "t50_ptmax",
              "t99_pt50", "t99_pt90", "t99_pt99", "t99_ptmax",
              "tmax_pt50", "tmax_pt90", "tmax_pt99", "tmax_ptmax").flatMap(
      n => List("max_cpus", "max_mem", "mean_cpu", "mean_mem").map(n + "_" + _)
    ) ++ List("num_submit", "num_schedule", "num_evict", "num_fail",
              "num_finish", "num_kill", "num_lost", "num_update_pending",
              "num_update_running", "num_missing", "num_events")
    converter(getValues, (stringNames, floatNames))
  }
}
