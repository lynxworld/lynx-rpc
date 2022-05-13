package org.grapheco.lynx.lynxrpc

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import org.grapheco.lynx.types.time.LynxDate
/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 4:34 下午 2022/1/23
 * @Modified By:
 */

/*
* The BaseSerializer is designed for basic data format.
* All the methods of it requires a byteBuf.
* */
trait BaseSerializer {

  protected def _encodeString(content: String, byteBuf: ByteBuf): ByteBuf = {
    val contentInBytes: Array[Byte] = content.getBytes
    val byteLength = contentInBytes.length
    if (byteLength < 32767) {
      byteBuf.writeByte(SerializerDataType.STRING.id.toByte)
      byteBuf.writeShort(byteLength)
    } else {
      byteBuf.writeByte(SerializerDataType.TEXT.id.toByte)
      byteBuf.writeInt(byteLength)
    }
    byteBuf.writeBytes(contentInBytes)
    byteBuf
  }

  protected def _encodeDate(date: LynxDate, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeByte(SerializerDataType.DATE.id.toByte)
    _encodeString(date.value.toString, byteBuf)
  }

  def encodeMap(map: Map[String, _], byteBuf: ByteBuf): ByteBuf = {
    val propNum: Int = map.size
    byteBuf.writeByte(propNum)
    map.foreach(kv => _writeKV(kv._1, kv._2, byteBuf))
    byteBuf
  }

  protected def _encodeStringList(byteBuf: ByteBuf, strList: List[String]): ByteBuf = {
    byteBuf.writeByte(SerializerDataType.ARRAY_STRING.id)
    val len: Int = strList.length
    byteBuf.writeInt(len)
    strList.foreach(str => _encodeString(str, byteBuf))
    byteBuf
  }

  protected def _encodeLongList(byteBuf: ByteBuf, longList: List[Long]): ByteBuf = {
    byteBuf.writeByte(SerializerDataType.ARRAY_LONG.id)
    val len: Int = longList.length
    byteBuf.writeInt(len)
    longList.foreach(long => byteBuf.writeLong(long))
    byteBuf
  }

  protected def _encodeDoubleList(byteBuf: ByteBuf, doubleList: List[Double]): ByteBuf = {
    byteBuf.writeByte(SerializerDataType.ARRAY_DOUBLE.id)
    val len: Int = doubleList.length
    byteBuf.writeInt(len)
    doubleList.foreach(double => byteBuf.writeDouble(double))
    byteBuf
  }

  protected def _encodeBooleanList(byteBuf: ByteBuf, booleanList: List[Boolean]): ByteBuf = {
    byteBuf.writeByte(SerializerDataType.ARRAY_BOOLEAN.id)
    val len: Int = booleanList.length
    byteBuf.writeInt(len)
    booleanList.foreach(boolean => byteBuf.writeBoolean(boolean))
    byteBuf
  }

  protected def _encodeAnyList(byteBuf: ByteBuf, anyList: List[_]): ByteBuf = {
    byteBuf.writeByte(SerializerDataType.ARRAY_ANY.id)
    val len: Int = anyList.length
    byteBuf.writeInt(len)
    anyList.foreach(any => _encodeAny(any, byteBuf))
    byteBuf
  }

  protected def _encodeAny(value: Any, byteBuf: ByteBuf): Unit = {
    value match {
      case longValue: Long => {
        byteBuf.writeByte(SerializerDataType.LONG.id.toByte)
        byteBuf.writeLong(longValue)
      }
      case doubleValue: Double => {
        byteBuf.writeByte(SerializerDataType.DOUBLE.id.toByte)
        byteBuf.writeDouble(doubleValue)
      }
      case stringValue: String => _encodeString(stringValue, byteBuf)
      case boolValue: Boolean => {
        byteBuf.writeByte(SerializerDataType.BOOLEAN.id.toByte)
        byteBuf.writeBoolean(boolValue)
      }
    }
  }

  protected def _writeKV(key: String, value: Any, byteBuf: ByteBuf): ByteBuf = {
    _encodeString(key, byteBuf)
    value match {
      case double: Double => {
        byteBuf.writeByte(SerializerDataType.DOUBLE.id.toByte)
        byteBuf.writeDouble(double)
      }
      case string: String => {
        _encodeString(string, byteBuf)
      }
      case long: Long => {
        byteBuf.writeByte(SerializerDataType.LONG.id.toByte)
        byteBuf.writeLong(long)
      }
      case bool: Boolean => {
        byteBuf.writeByte(SerializerDataType.BOOLEAN.id.toByte)
        byteBuf.writeBoolean(bool)
      }
      case date: LynxDate => {
        byteBuf.writeByte(SerializerDataType.DATE.id.toByte)
        _encodeString(date.value.toString, byteBuf)
      }

      case _ => throw new Exception("Unexpected data type.")
    }
    byteBuf
  }

}




