package org.grapheco.lynx.lynxrpc

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxString}
import org.grapheco.lynx.types.time.LynxDate

import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:27 2022/4/14
 * @Modified By:
 */
trait BaseDeserializer {

  protected def _decodeDate(byteBuf: ByteBuf): LynxDate = {
    byteBuf.readByte
    val dataInStr: String = _decodeStringWithoutFlag(byteBuf)
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val date = LocalDate.parse(dataInStr, fmt)
    LynxDate(date)
  }

  protected def _decodeStringWithoutFlag(byteBuf: ByteBuf): String = {
    val len: Int = byteBuf.readShort().toInt
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
    byteBuf.readBytes(bos, len)
    bos.toString
  }

  protected def _decodeStringWithFlag(byteBuf: ByteBuf): String = {
    byteBuf.readByte() // drop the String Flag
    _decodeStringWithoutFlag(byteBuf)
  }

  def decodePropMap[_](byteBuf: ByteBuf): Map[Int, _] = {
    val propNum: Int = byteBuf.readByte().toInt
    val propsMap: Map[Int, _] = new Array[Int](propNum).map(item => {
      val propId: Int = byteBuf.readInt()
      val propType: Int = byteBuf.readByte().toInt
      val propValue = SerializerDataType(propType) match {
        case SerializerDataType.STRING => _decodeStringWithoutFlag(byteBuf)
        case SerializerDataType.INT => byteBuf.readInt()
        case SerializerDataType.LONG => byteBuf.readLong()
        case SerializerDataType.DOUBLE => byteBuf.readDouble()
        case SerializerDataType.FLOAT => byteBuf.readFloat()
        case SerializerDataType.BOOLEAN => byteBuf.readBoolean()
        case SerializerDataType.DATE => _decodeDate(byteBuf)
        case SerializerDataType.ARRAY_STRING => decodeArray[String](byteBuf, SerializerDataType.ARRAY_STRING)
        case SerializerDataType.ARRAY_INT => decodeArray[Int](byteBuf, SerializerDataType.ARRAY_INT)
        case SerializerDataType.ARRAY_LONG => decodeArray[Long](byteBuf, SerializerDataType.ARRAY_LONG)
        case SerializerDataType.ARRAY_DOUBLE => decodeArray[Double](byteBuf, SerializerDataType.ARRAY_DOUBLE)
        case SerializerDataType.ARRAY_FLOAT => decodeArray[Float](byteBuf, SerializerDataType.ARRAY_FLOAT)
        case SerializerDataType.ARRAY_BOOLEAN => decodeArray[Boolean](byteBuf, SerializerDataType.ARRAY_BOOLEAN)
        case SerializerDataType.ARRAY_ANY => decodeArray[Any](byteBuf, SerializerDataType.ARRAY_ANY)
        case _ => throw new Exception(s"Unexpected TypeId ${propType}")
      }
      propId -> propValue
    }).toMap
    propsMap
  }

  def decodeArray[T](byteBuf: ByteBuf, propType: SerializerDataType.Value): Array[LynxValue] = {
    val length = byteBuf.readInt()
    propType match {
      case SerializerDataType.ARRAY_STRING => new Array[String](length).map(_ => {
        // This byte is the Flag of String.
        byteBuf.readByte()
        LynxString(_decodeStringWithoutFlag(byteBuf))
      })
      case SerializerDataType.ARRAY_DOUBLE => new Array[Double](length).map(_ => LynxFloat(byteBuf.readDouble()))
      case SerializerDataType.ARRAY_LONG => new Array[Long](length).map(_ => LynxInteger(byteBuf.readLong()))
      case SerializerDataType.ARRAY_BOOLEAN => new Array[Boolean](length).map(_ => LynxBoolean(byteBuf.readBoolean()))
      case SerializerDataType.ARRAY_ANY => new Array[Any](length).map(_ => LynxValue(_decodeAny(byteBuf)))
    }
  }

  protected def _decodeAny[_](byteBuf: ByteBuf): Any = {
    val typeFlag = SerializerDataType(byteBuf.readByte().toInt)
    typeFlag match {
      case SerializerDataType.INT => byteBuf.readInt()
      case SerializerDataType.LONG => byteBuf.readLong()
      case SerializerDataType.STRING => _decodeStringWithFlag(byteBuf)
      case SerializerDataType.DOUBLE => byteBuf.readDouble()
      case SerializerDataType.FLOAT => byteBuf.readFloat()
      case SerializerDataType.BOOLEAN => byteBuf.readBoolean()
      case _ => throw new Exception(s"Unexpected type for typeId ${typeFlag.id}")
    }
  }

}
