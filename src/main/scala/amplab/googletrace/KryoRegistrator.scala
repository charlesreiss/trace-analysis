package amplab.googletrace

import Protos._
import TraceUtil._

import com.google.protobuf.Message
import com.google.protobuf.{CodedInputStream, CodedOutputStream}

import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import com.esotericsoftware.kryo.serialize.ArraySerializer
import com.esotericsoftware.kryo.compress.DeflateCompressor

import java.nio.ByteBuffer

class KryoRegistrator extends spark.KryoRegistrator {
  def registerClasses(kyro: Kryo): Unit = {
    KryoRegistrator.realRegisterClasses(kyro)
  }
}

object KryoRegistrator {
  val tlBuffer = new java.lang.ThreadLocal[Array[Byte]] {
    override def initialValue: Array[Byte] = new Array[Byte](1024 * 1024 * 128)
  }
  abstract class PBSerialize[T <: Message] extends KSerializer {
    override final def writeObjectData(buf: ByteBuffer, _obj: AnyRef) {
      val obj = _obj.asInstanceOf[T]
      val tempBuf = tlBuffer.get
      val codedOut = CodedOutputStream.newInstance(tempBuf)
      obj.writeTo(codedOut)
      val len = obj.getSerializedSize
      assert(len == tempBuf.size - codedOut.spaceLeft)
      buf.putInt(obj.getSerializedSize)
      buf.put(tempBuf, 0, len)
    }
    def parseFrom(in: CodedInputStream): T
    override final def readObjectData[U](buf: ByteBuffer, cls: Class[U]): U = {
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
    kyro.register(classOf[MachineAttribute], new PBSerialize[MachineAttribute] {
      final override def parseFrom(in: CodedInputStream) =
          MachineAttribute.parseFrom(in)
    })
    kyro.register(classOf[MachineConstraint], new PBSerialize[MachineConstraint] {
      final override def parseFrom(in: CodedInputStream) =
          MachineConstraint.parseFrom(in)
    })
    kyro.register(classOf[UsageByMachine], new PBSerialize[UsageByMachine] {
      final override def parseFrom(in: CodedInputStream) =
          UsageByMachine.parseFrom(in)
    })
    kyro.register(classOf[TaskUtilization], new PBSerialize[TaskUtilization] {
      final override def parseFrom(in: CodedInputStream) =
          TaskUtilization.parseFrom(in)
    })
    kyro.register(classOf[JobUtilization], new PBSerialize[JobUtilization] {
      final override def parseFrom(in: CodedInputStream) =
          JobUtilization.parseFrom(in)
    })
    kyro.register(classOf[TaskInfo], new PBSerialize[TaskInfo] {
      final override def parseFrom(in: CodedInputStream) =
          TaskInfo.parseFrom(in)
    })
    for (cls <- List(classOf[Array[TaskUsage]], classOf[Array[TaskEvent]],
                     classOf[Array[JobEvent]], classOf[Array[MachineEvent]],
                     classOf[Array[TaskInfo]], classOf[Array[JobUtilization]],
                     classOf[Array[TaskUtilization]])) {
      kyro.register(cls, new DeflateCompressor(new ArraySerializer(kyro)))
    }
  }
}
