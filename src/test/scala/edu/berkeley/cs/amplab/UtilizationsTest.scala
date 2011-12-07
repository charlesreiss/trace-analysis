package edu.berkeley.cs.amplab

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import spark.SparkContext

import Utilizations._
import TraceUtil._
import GoogleTrace._

import scala.math.abs

import com.google.protobuf.TextFormat

class UtilizationsTestSuite extends FunSuite with ShouldMatchers {
  test("getPercentile on 1 element") {
    val single = Array(42.0f)
    assert(42.0f === getPercentile(0.0, single))
    assert(42.0f === getPercentile(1.0, single))
    assert(42.0f === getPercentile(0.5, single))
    assert(42.0f === getPercentile(0.3, single))
  }

  test("getPercentile on 2 elements") {
    val two = Array(0.0f, 2.0f)  // getPercentile assumes sortedness
    assert(0.0f === getPercentile(0.0, two))
    assert(2.0f === getPercentile(1.0, two))
    assert(1.0f === getPercentile(0.5, two))
    abs(0.6f - getPercentile(0.3, two)) should be < (1e-4f)
  }

  test("getPercentile on 3 elements") {
    val three = Array(0.0f, 2.0f, 5.0f)
    assert(0.0f === getPercentile(0.0, three))
    assert(5.0f === getPercentile(1.0, three))
    assert(2.0f === getPercentile(0.5, three))
    abs(3.5f - getPercentile(0.75, three)) should be < (1e-4f)
    abs(1.0f - getPercentile(0.25, three)) should be < (1e-4f)
  }

  test("getTaskUtilization missing request") {
    val usageBuilder = TaskUsage.newBuilder
    TextFormat.merge("""
      start_time: 0 end_time: 10
      resources: < cpus: 0.5 memory: 0.25 >
      max_resources: < cpus: 0.75 memory: 0.125 >
      task_info: < job: < id: 42 > task_index: 10 >
    """, usageBuilder)
    assert(None == getTaskUtilization(Seq(usageBuilder.build)))
  }

  test("getTaskUtilization on 1 record") {
    val usageBuilder = TaskUsage.newBuilder
    TextFormat.merge("""
      start_time: 0 end_time: 10
      resources: < cpus: 0.5 memory: 0.25 >
      max_resources: < cpus: 0.75 memory: 0.125 >
      task_info: < job: < id: 42 > task_index: 10
        requested_resources: <
          cpus: 0.25 memory: 0.75
        >
      >
    """, usageBuilder)
    val usage = usageBuilder.build

    val expectUtilBuilder = TaskUtilization.newBuilder
    TextFormat.merge("""
      info: <
        job: < id: 42 > task_index: 10
        requested_resources: <
          cpus: 0.25 memory: 0.75
        >
      >

      min_request: <
        cpus: 0.25 memory: 0.75
      >
      max_request: <
        cpus: 0.25 memory: 0.75
      >
      
      usage_percentile: 0.5
      usage_percentile: 0.9
      usage_percentile: 0.99
      usage_percentile: 1.0
      percentile_task_usage: < cpus: 0.75 memory: 0.125 >
      percentile_task_usage: < cpus: 0.75 memory: 0.125 >
      percentile_task_usage: < cpus: 0.75 memory: 0.125 >
      percentile_task_usage: < cpus: 0.75 memory: 0.125 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >

      start_time: 0
      end_time: 10
      running_time: 10
    """, expectUtilBuilder)
    assert(Some(expectUtilBuilder.build) === getTaskUtilization(Seq(usage)))
  }

  test("getTaskUtilization on 2 records") {
    val usageBuilder = TaskUsage.newBuilder
    TextFormat.merge("""
      start_time: 0 end_time: 10
      resources: < cpus: 0.5 memory: 0.25 >
      max_resources: < cpus: 1.0 memory: 1.0 >
      task_info: < job: < id: 42 > task_index: 10
        requested_resources: <
          cpus: 0.25 memory: 0.75
        >
      >
    """, usageBuilder)
    val usage1 = usageBuilder.build
    usageBuilder.clear
    TextFormat.merge("""
      start_time: 20 end_time: 40
      resources: < cpus: 0.25 memory: 0.125 >
      max_resources: < cpus: 2.0 memory: 2.0 >
      task_info: < job: < id: 42 > task_index: 10
        requested_resources: <
          cpus: 0.5 memory: 0.25
        >
      >
    """, usageBuilder)
    val usage2 = usageBuilder.build

    /* TODO(Charles): weighted average by runtime */
    val expectUtilBuilder = TaskUtilization.newBuilder
    TextFormat.merge("""
      info: <
        job: < id: 42 > task_index: 10
        requested_resources: <
          cpus: 0.25 memory: 0.75
        >
      >

      min_request: <
        cpus: 0.25 memory: 0.25
      >
      max_request: <
        cpus: 0.5 memory: 0.75
      >
      
      usage_percentile: 0.5
      usage_percentile: 0.9
      usage_percentile: 0.99
      usage_percentile: 1.0

      start_time: 0
      end_time: 40
      running_time: 30
    """, expectUtilBuilder)
    for (percentile <- PER_TASK_PERCENTILES) {
      def f(d: Double) = d.asInstanceOf[Float]
      expectUtilBuilder.addPercentileTaskUsage(
        Resources.newBuilder.setCpus(f(1.0 + percentile)).
          setMemory(f(1.0 + percentile))
      ).addPercentileMeanTaskUsage(
        Resources.newBuilder.setCpus(f(0.25 + percentile / 4.0)).
          setMemory(f(0.125 + percentile / 8.0))
      )
    }
    assert(Some(expectUtilBuilder.build) ===
           getTaskUtilization(Seq(usage1, usage2)))
  }

  test("getJobUtilization on 1 record") {
    val taskUtilBuilder = TaskUtilization.newBuilder
    TextFormat.merge("""
      info: <
        job: < id: 42 > task_index: 10
        requested_resources: <
          cpus: 0.25 memory: 0.75
        >
      >

      min_request: <
        cpus: 0.25 memory: 0.75
      >
      max_request: <
        cpus: 0.25 memory: 0.75
      >
      
      usage_percentile: 0.5
      usage_percentile: 1.0
      percentile_task_usage: < cpus: 1.0 memory: 0.125 >
      percentile_task_usage: < cpus: 0.75 memory: 0.125 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >

      start_time: 0
      end_time: 10
      running_time: 10
    """, taskUtilBuilder)
    val jobUtilBuilder = JobUtilization.newBuilder
    TextFormat.merge("""
      job_info: < id: 42 >
      num_tasks: 1
      min_request: <
        cpus: 0.25 memory: 0.75
      >
      max_request: <
        cpus: 0.25 memory: 0.75
      >

      task_percentile: 0.5
      task_percentile: 0.99
      task_percentile: 1.0
      task_percentile: 0.5
      task_percentile: 0.99
      task_percentile: 1.0
      usage_percentile: 0.5
      usage_percentile: 0.5
      usage_percentile: 0.5
      usage_percentile: 1.0
      usage_percentile: 1.0
      usage_percentile: 1.0
      percentile_task_usage: < cpus: 1.0 memory: 0.125 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
      percentile_task_usage: < cpus: 1.0 memory: 0.125 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
      percentile_task_usage: < cpus: 1.0 memory: 0.125 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >

      percentile_task_usage: < cpus: 0.75 memory: 0.125 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
      percentile_task_usage: < cpus: 0.75 memory: 0.125 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
      percentile_task_usage: < cpus: 0.75 memory: 0.125 >
      percentile_mean_task_usage: < cpus: 0.5 memory: 0.25 >
    """, jobUtilBuilder)
    assert(jobUtilBuilder.build ===
      getJobUtilization(Seq(taskUtilBuilder.build)))
  }
}

// vim: c_minlines=1000
