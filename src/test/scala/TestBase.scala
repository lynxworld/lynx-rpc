import org.grapheco.lynx.lynxrpc.{LynxByteBufFactory, LynxValueDeserializer, LynxValueSerializer}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.junit.Assert

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:49 2022/4/27
 * @Modified By:
 */
class TestBase {

  protected def _testFunc(input: Any): Unit ={
    val lynxValue: LynxValue = input match {
      case lV: LynxValue => lV
      case _ => LynxValue(input)
    }
    val deserializedLynxValue: LynxValue = _getDeserializedLynxValue(input)
    _compareLynxValue(lynxValue, deserializedLynxValue)
  }

  protected def _compareLynxValue(input: LynxValue, deserialized: LynxValue): Unit = {
    input match {
      case lynxInteger: LynxInteger => Assert.assertEquals(lynxInteger.value, deserialized.value)
      case lynxFloat: LynxFloat => Assert.assertEquals(lynxFloat.value, deserialized.value)
      case lynxString: LynxString => Assert.assertEquals(lynxString.value, deserialized.value)
      case lynxBoolean: LynxBoolean => Assert.assertEquals(lynxBoolean.value, deserialized.value)
      case lynxList: LynxList => Assert.assertTrue(_compareLynxList(lynxList, deserialized.asInstanceOf[LynxList]))
      case lynxMap: LynxMap => Assert.assertTrue(_compareLynxMap(lynxMap, deserialized.asInstanceOf[LynxMap]))
      case lynxNode: LynxNode => {
        val expectedNode: LynxNode = lynxNode
        val actualNode: LynxNode = deserialized.asInstanceOf[LynxNode]
        val expectedLabels: Array[String] = expectedNode.labels.map(lynxNodeLabel => lynxNodeLabel.value).toArray
        val actualLabels: Array[String] = actualNode.labels.map(lynxNodeLabel => lynxNodeLabel.value).toArray
        val expectedPropMap: LynxMap = LynxMap(expectedNode.keys.map(key => key.value -> expectedNode.property(key).get).toMap)
        val actualPropMap: LynxMap = LynxMap(actualNode.keys.map(key => key.value -> expectedNode.property(key).get).toMap)

        Assert.assertEquals(expectedNode.id.toLynxInteger.value, actualNode.value.id.toLynxInteger.value)
        expectedLabels.zip(actualLabels).foreach(pair => Assert.assertEquals(pair._1, pair._2))
        _compareLynxMap(expectedPropMap, actualPropMap)
      }

      case lynxRelationship: LynxRelationship => {
        val expectedRelationship: LynxRelationship = lynxRelationship
        val actualRelationship: LynxRelationship = deserialized.asInstanceOf[LynxRelationship]

        val expectedType: String = expectedRelationship.relationType.get.value
        val actualType: String = actualRelationship.relationType.get.value

        val expectedStartId: Long = expectedRelationship.startNodeId.toLynxInteger.value
        val actualStartId: Long = actualRelationship.startNodeId.toLynxInteger.value

        val expectedEndId: Long = expectedRelationship.endNodeId.toLynxInteger.value
        val actualEndId: Long = actualRelationship.endNodeId.toLynxInteger.value

        val expectedPropsMap: LynxMap = LynxMap(expectedRelationship.keys.map(key => key.value -> expectedRelationship.property(key).get).toMap)
        val actualPropsMap: LynxMap = LynxMap(actualRelationship.keys.map(key => key.value -> lynxRelationship.property(key).get).toMap)

        Assert.assertEquals(expectedRelationship.id.toLynxInteger.value, actualRelationship.id.toLynxInteger.value)
        Assert.assertEquals(expectedType, actualType)
        Assert.assertEquals(expectedStartId, actualStartId)
        Assert.assertEquals(expectedEndId, actualEndId)
        _compareLynxMap(expectedPropsMap, actualPropsMap)
      }
    }
  }

  protected def _getDeserializedLynxValue(input: Any): LynxValue = {
    val byteBuf = LynxByteBufFactory.getByteBuf
    val serializer = new LynxValueSerializer
    val deSerializer = new LynxValueDeserializer
    serializer.encodeLynxValue(byteBuf, LynxValue(input))
    val bytes = LynxByteBufFactory.exportBuf(byteBuf)
    val deSerializedValue = deSerializer.decodeLynxValue(LynxByteBufFactory.fromBytes(bytes))
    LynxByteBufFactory.releaseBuf(byteBuf)
    deSerializedValue
  }

  protected def _compareLynxList(x: LynxList, y: LynxList): Boolean = {
    val xList: List[LynxValue] = x.value
    val yList: List[LynxValue] = y.value
    if(xList.length != yList.length) return false
    else {
      var equal: Boolean = true
      val xIter: Iterator[LynxValue] = xList.toIterator
      val yIter: Iterator[LynxValue] = yList.toIterator
      while(xIter.hasNext && yIter.hasNext) {
        equal = (xIter.next(), yIter.next()) match {
          case (x: LynxNumber, y: LynxNumber) => x.value == y.value
          case (x: LynxString, y: LynxString) => x.value == y.value
          case (x: LynxBoolean, y: LynxBoolean) => x.value == y.value
          case (x: LynxList, y: LynxList) => _compareLynxList(x, y)
          case (x: LynxMap, y: LynxMap) => _compareLynxMap(x, y)
          case _ => false
        }
        if(!equal) return false
      }
      equal
    }
  }

  // Note: The elements of the two maps should be in same sort.
  protected def _compareLynxMap(x: LynxMap, y: LynxMap): Boolean = {
    val xMap: Map[String, LynxValue] = x.value
    val yMap: Map[String, LynxValue] = y.value
    val keysEqual: Boolean = xMap.keys.sameElements(yMap.keys)
    val valuesEqual: Boolean = _compareLynxList(LynxList(xMap.values.toList), LynxList(yMap.values.toList))
    keysEqual && valuesEqual
  }

}
