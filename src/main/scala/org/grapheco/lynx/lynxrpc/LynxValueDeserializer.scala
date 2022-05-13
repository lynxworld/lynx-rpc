package org.grapheco.lynx.lynxrpc

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:27 2022/4/14
 * @Modified By:
 */
class LynxValueDeserializer extends BaseDeserializer {
  def decodeLynxValue(byteBuf: ByteBuf): LynxValue = {
    val typeFlag: SerializerDataType.Value = SerializerDataType(byteBuf.readByte().toInt)
    typeFlag match {
      case SerializerDataType.LONG => LynxInteger(byteBuf.readLong())
      case SerializerDataType.DOUBLE => LynxFloat(byteBuf.readDouble())
      case SerializerDataType.STRING => LynxString(_decodeStringWithFlag(byteBuf))
      case SerializerDataType.BOOLEAN => LynxBoolean(byteBuf.readBoolean())
      case SerializerDataType.LYNXLIST => _decodeLynxList(byteBuf)
      case SerializerDataType.LYNXMAP => _decodeLynxMap(byteBuf)
      case SerializerDataType.LYNXNODE => _decodeLynxNode(byteBuf)
      case SerializerDataType.LYNXRELATIONSHIP => _decodeLynxRelationship(byteBuf)
    }
  }

  // [flag(Byte)],[length(Content)]
  private def _decodeLynxList(byteBuf: ByteBuf): LynxList = {
    val typeFlag: SerializerDataType.Value = SerializerDataType(byteBuf.readByte().toInt)
    val length: Int = byteBuf.readInt()
    val value: List[LynxValue] = typeFlag match {
      case SerializerDataType.ARRAY_LONG => new Array[Long](length).map(_ => LynxInteger(byteBuf.readLong())).toList
      case SerializerDataType.ARRAY_DOUBLE => new Array[Double](length).map(_ => LynxFloat(byteBuf.readDouble())).toList
      case SerializerDataType.ARRAY_STRING => new Array[String](length).map(_ => LynxString(_decodeStringWithFlag(byteBuf))).toList
      case SerializerDataType.ARRAY_BOOLEAN => new Array[Boolean](length).map(_ => LynxBoolean(byteBuf.readBoolean())).toList
      case SerializerDataType.ARRAY_ANY => new Array[Any](length).map(_ => decodeLynxValue(byteBuf)).toList
      case SerializerDataType.ARRAY_ARRAY => new Array[Int](length).map(_ => _decodeLynxList(byteBuf)).toList
    }
    LynxList(value)
  }

  private def _decodeLynxMap(byteBuf: ByteBuf): LynxMap = {
    val typeFlag: Int = byteBuf.readByte().toInt
    val length: Int = byteBuf.readInt()
    LynxMap(new Array[Int](length).map(_ => _decodeStringWithFlag(byteBuf) -> decodeLynxValue(byteBuf)).toMap)
  }

  private def _decodeLynxNode(bytebuf: ByteBuf): LynxNode = {
    new LynxNode {
      val _id: LynxInteger = decodeLynxValue(bytebuf).asInstanceOf[LynxInteger]
      val _labels: List[LynxString] = decodeLynxValue(bytebuf).asInstanceOf[LynxList].value.asInstanceOf[List[LynxString]]
      val _props: Map[String, LynxValue] = decodeLynxValue(bytebuf).asInstanceOf[LynxMap].value

      override val id: LynxId = new LynxIdImpl(_id)

      override def labels: Seq[LynxNodeLabel] = {
        _labels.map(lynxString => LynxNodeLabel(lynxString.value))
      }

      override def keys: Seq[LynxPropertyKey] = {
        _props.keys.map(LynxPropertyKey).toSeq
      }

      override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = {
        _props.get(propertyKey.value)
      }
    }
  }

  private def _decodeLynxRelationship(byteBuf: ByteBuf): LynxRelationship = {
    new LynxRelationship {
      val _id: LynxInteger = decodeLynxValue(byteBuf).asInstanceOf[LynxInteger]
      val _relationshipType: LynxRelationshipType = LynxRelationshipType(decodeLynxValue(byteBuf).asInstanceOf[LynxString].value)
      val _startId: LynxInteger = decodeLynxValue(byteBuf).asInstanceOf[LynxInteger]
      val _endId: LynxInteger = decodeLynxValue(byteBuf).asInstanceOf[LynxInteger]
      val _props: Map[String, LynxValue] = decodeLynxValue(byteBuf).asInstanceOf[LynxMap].value
      override val id: LynxId = new LynxId {
        override val value: Any = _id

        override def toLynxInteger: LynxInteger = _id
      }
      override val startNodeId: LynxId = new LynxIdImpl(_startId)
      override val endNodeId: LynxId = new LynxIdImpl(_endId)

      override def relationType: Option[LynxRelationshipType] = Some(_relationshipType)

      override def keys: Seq[LynxPropertyKey] = _props.keys.map(LynxPropertyKey).toSeq

      override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = _props.get(propertyKey.value)
    }
  }

}
