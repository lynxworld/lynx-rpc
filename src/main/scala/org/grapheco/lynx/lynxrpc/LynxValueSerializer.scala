package org.grapheco.lynx.lynxrpc

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxRelationship}
import shapeless.TypeCase

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 17:46 2022/4/13
 * @Modified By:
 */
class LynxValueSerializer extends BaseSerializer {

  def encodeLynxValue(byteBuf: ByteBuf, value: LynxValue): ByteBuf = {
    value match {
      // The Int is regarded as long in LynxValue system.
      case lynxInteger: LynxInteger =>
        byteBuf.writeByte(SerializerDataType.LONG.id)
        byteBuf.writeLong(lynxInteger.value)
      case lynxDouble: LynxFloat =>
        byteBuf.writeByte(SerializerDataType.DOUBLE.id)
        byteBuf.writeDouble(lynxDouble.value)
      case lynxString: LynxString =>
        byteBuf.writeByte(SerializerDataType.STRING.id)
        _encodeString(lynxString.value, byteBuf)
      case lynxBoolean: LynxBoolean =>
        byteBuf.writeByte(SerializerDataType.BOOLEAN.id)
        byteBuf.writeBoolean(lynxBoolean.value)
      case lynxList: LynxList =>
        byteBuf.writeByte(SerializerDataType.LYNXLIST.id)
        _encodeLynxList(byteBuf, lynxList)
      case lynxMap: LynxMap =>
        byteBuf.writeByte(SerializerDataType.LYNXMAP.id)
        _encodeLynxMap(byteBuf, lynxMap)
      case lynxNode: LynxNode => _encodeLynxNode(byteBuf, lynxNode)
      case lynxRelationship: LynxRelationship => _encodeLynxRelationship(byteBuf, lynxRelationship)
      case _ => throw new Exception(s"Unexpected type of ${value}")
    }
    byteBuf
  }

  // Caution: Do not write the typeFlag of LynxList again.
  private def _encodeLynxList(byteBuf: ByteBuf, lynxList: LynxList): ByteBuf = {
    val AllLynxInteger = TypeCase[List[LynxInteger]]
    val AllLynxFloat = TypeCase[List[LynxFloat]]
    val AllLynxString = TypeCase[List[LynxString]]
    val AllLynxBoolean = TypeCase[List[LynxBoolean]]
    val AllLynxList = TypeCase[List[LynxList]]
    val AllLynxValue = TypeCase[List[LynxValue]] // Similar to List[Any]

    lynxList.value match {
      case AllLynxInteger(intList) => _encodeLongList(byteBuf, intList.map(lynxInteger => lynxInteger.value))
      case AllLynxFloat(doubleList) => _encodeDoubleList(byteBuf, doubleList.map(lynxDouble => lynxDouble.value))
      case AllLynxString(stringList) => _encodeStringList(byteBuf, stringList.map(lynxString => lynxString.value))
      case AllLynxBoolean(booleanList) => _encodeBooleanList(byteBuf, booleanList.map(lynxBoolean => lynxBoolean.value))
      case AllLynxList(listList) => {
        byteBuf.writeByte(SerializerDataType.ARRAY_ARRAY.id)
        byteBuf.writeInt(listList.length)
        listList.foreach(list => _encodeLynxList(byteBuf, list))
        byteBuf
      }

      // Caution: This case should be regarded the wildcard-case, so should be at last.
      case AllLynxValue(anyList) => {
        byteBuf.writeByte(SerializerDataType.ARRAY_ANY.id)
        byteBuf.writeInt(anyList.length)
        anyList.foreach(anyLynxValue => encodeLynxValue(byteBuf, anyLynxValue))
        byteBuf
      }
    }
  }

  // LynxMap: [String, LynxValue]
  private def _encodeLynxMap(byteBuf: ByteBuf, lynxMap: LynxMap): ByteBuf = {
    val mapLength: Int = lynxMap.value.size
    byteBuf.writeByte(SerializerDataType.LYNXMAP.id)
    byteBuf.writeInt(mapLength)
    lynxMap.value.foreach(kv => {
      _encodeString(kv._1, byteBuf)
      encodeLynxValue(byteBuf, kv._2)
    })
    byteBuf
  }

  private def _encodeLynxNode(byteBuf: ByteBuf, node: LynxNode): ByteBuf = {
    val id: LynxId = node.value.id
    val labels: List[LynxString] = node.value.labels.map(lynxNodeLabel => LynxString(lynxNodeLabel.value)).toList
    // TODO Warning: The prop may not exist.
    val propsMap: Map[String, LynxValue] = node.value.keys.map(key => key.value -> node.value.property(key).get).toMap
    byteBuf.writeByte(SerializerDataType.LYNXNODE.id)
    encodeLynxValue(byteBuf, id.toLynxInteger)
    encodeLynxValue(byteBuf, LynxList(labels))
    encodeLynxValue(byteBuf, LynxMap(propsMap))
  }

  private def _encodeLynxRelationship(byteBuf: ByteBuf, relationship: LynxRelationship): ByteBuf = {
    val id = relationship.value.id
    // TODO Warning the relationshiptype may not exist.
    val relationshipType: String = relationship.relationType.get.value
    val startId: LynxId = relationship.startNodeId
    val endId: LynxId = relationship.endNodeId
    val propsMap: Map[String, LynxValue] = relationship.value.keys.map(key => key.value -> relationship.value.property(key).get).toMap
    byteBuf.writeByte(SerializerDataType.LYNXRELATIONSHIP.id)
    encodeLynxValue(byteBuf, id.toLynxInteger)
    encodeLynxValue(byteBuf, LynxString(relationshipType))
    encodeLynxValue(byteBuf, startId.toLynxInteger)
    encodeLynxValue(byteBuf, endId.toLynxInteger)
    encodeLynxValue(byteBuf, LynxMap(propsMap))
  }

}
