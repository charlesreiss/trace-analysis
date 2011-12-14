package amplab.googletrace

import Protos._
import TraceUtil._

import spark.RDD
import spark.SparkContext._

object TaskAggregates {
  // Returns TaskInfo to merge into existing TaskInfos. This will be a 
  // deliberately incomplete TaskInfo.
  def consolidateInfoFrom(_events: Seq[TaskEvent]): TaskInfo = {
    val events = _events.sortBy(_.getTime)
    val builder = TaskInfo.newBuilder
    val eventTypes = events.map(_.getType).groupBy(identity).mapValues(_.size)
    for (t <- TaskEventType.values) {
      builder.addNumEventsByType(0)
    }
    for (t <- TaskEventType.values) {
      builder.setNumEventsByType(t.getNumber, eventTypes.getOrElse(t, 0))
    }
    builder.setFinalEvent(events.last.getType)
    events.filter(_.getType == TaskEventType.SUBMIT).headOption.foreach(
      e => builder.setFirstSubmitTime(e.getTime))
    events.filter(_.getType == TaskEventType.SCHEDULE).headOption.foreach(
      e => builder.setFirstScheduleTime(e.getTime))
    events.filter(e => e.getType == TaskEventType.EVICT ||
                       e.getType == TaskEventType.FAIL ||
                       e.getType == TaskEventType.FINISH ||
                       e.getType == TaskEventType.KILL ||
                       e.getType == TaskEventType.LOST).headOption.foreach(
      e => builder.setLastDescheduleTime(e.getTime))
    builder.setNumMissing(events.map(_.hasMissingType).size)
    builder.setNumEvents(events.size)
    return builder.buildPartial
  }

  def markTasks(inEvents: RDD[TaskEvent]): RDD[TaskEvent] = {
    def markOne(events: Seq[TaskEvent]): Seq[TaskEvent] = {
      val extraInfo = consolidateInfoFrom(events)
      events.map(event => {
        val builder = event.toBuilder
        builder.getInfoBuilder.mergeFrom(extraInfo)
        builder.build
      })
    }
    import Join.keyByTask

    inEvents.map(keyByTask).groupByKey.mapValues(markOne).flatMap(_._2)
  }

  def getTaskInfoSamples(inEvents: RDD[TaskEvent]): RDD[(Long, Seq[TaskInfo])] = {
    assert(false)
    null
  }
}
