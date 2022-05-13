import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property._
import org.junit.runners.MethodSorters
import org.junit.{FixMethodOrder, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:07 2022/4/14
 * @Modified By:
 */

@FixMethodOrder(MethodSorters.JVM)
class LynxSerializerTest extends TestBase {

  //Test LynxInteger
  @Test
  def test1(): Unit = {
    _testFunc(0)
    _testFunc(1)
    _testFunc(-1)
    _testFunc(999999999999l)
  }

  //Test LynxDouble
  @Test
  def test2(): Unit = {
    _testFunc(0.1)
    _testFunc(0.1D)
    _testFunc(-0.1)
    _testFunc(100.0)
  }

  //Test LynxBoolean
  @Test
  def test3(): Unit = {
    _testFunc(true)
    _testFunc(false)
  }

  //Test LynxString
  @Test
  def test4(): Unit = {
    _testFunc("")
    _testFunc("-")
    _testFunc("$%^")
    //ToDo: Test a long String.
  }

  //Test LynxList[LynxInteger]
  @Test
  def test5(): Unit = {
    val lynxList1 = LynxList(List(1, 2, 3, 4).map(LynxValue(_)))
    val lynxList2 = LynxList(List(2, 3, 4, 1).map(LynxValue(_)))
    _testFunc(lynxList1)
    _testFunc(lynxList2)
  }

  //Test LynxList[LynxDouble]
  @Test
  def test6(): Unit = {
    val lynxList = LynxList(List(1.0, 2.0D, -1.3d).map(LynxValue(_)))
    _testFunc(lynxList)
  }

  //Test LynxList[LynxBoolean]
  @Test
  def test7(): Unit = {
    val lynxList = LynxList(List(true, false, false, true).map(LynxBoolean(_)))
    _testFunc(lynxList)
  }

  //Test LynxList[Any]
  @Test
  def test8(): Unit = {
    val lynxList = LynxList(List(1, 2.0, "", "123?", -1.0D, true).map(LynxValue(_)))
    _testFunc(lynxList)
  }

  //Test LynxList[LynxList]
  @Test
  def test9(): Unit = {
    val lynxList = LynxList(List(1, 2.0, "", "123?", -1.0D, true).map(LynxValue(_)))
    val nestedLynxList = LynxList(List(lynxList, LynxValue("test str"), LynxValue(0.1), LynxValue(true)))
    _testFunc(nestedLynxList)
  }

  //Test LynxMap[String, LynxNumber]
  @Test
  def test10(): Unit = {
    val numberMap: Map[String, LynxValue] = Map("one" -> LynxInteger(1), "0.5" -> LynxFloat(0.5), "-1" -> LynxInteger(-1))
    val lynxNubmerMap: LynxMap = LynxMap(numberMap)
    _testFunc(lynxNubmerMap)
  }

  //Test LynxMap[String, LynxBoolean]
  @Test
  def test11(): Unit = {
    val booleanMap: Map[String, LynxBoolean] = Map("it is true" -> LynxBoolean(true), "it is false" -> LynxBoolean(false))
    val lynxBooleanMap: LynxMap = LynxMap(booleanMap)
    _testFunc(lynxBooleanMap)
  }

  //Test LynxMap[String, LynxString]
  @Test
  def test12(): Unit = {
    val stringMap: Map[String, LynxString] = Map("string" -> LynxString("string"), "" -> LynxString(""))
    val lynxStringMap: LynxMap = LynxMap(stringMap)
    _testFunc(lynxStringMap)
  }

  //Test LynxMap[String, LynxValue]
  @Test
  def test13(): Unit = {
    val valueMap: Map[String, LynxValue] = Map("true" -> LynxBoolean(true),
      "string" -> LynxString("string"), "123" -> LynxInteger(123), "1.0" -> LynxFloat(1.0D))
    val lynxMap: LynxMap = LynxMap(valueMap)
    _testFunc(lynxMap)
  }

  //Test LynxMap[String, LynxMap]
  @Test
  def test14(): Unit = {
    val innerMap: Map[String, LynxValue] = Map("true" -> LynxBoolean(true),
      "string" -> LynxString("string"), "123" -> LynxInteger(123), "1.0" -> LynxFloat(1.0D))
    val map: Map[String, LynxValue] = Map("nested map" -> LynxMap(innerMap))
    val lynxMap: LynxMap = LynxMap(map)
    _testFunc(lynxMap)
  }

  //Test LynxMap[String, LynxList]
  @Test
  def test15(): Unit = {
    val lynxList = LynxList(List(1, 2.0, "", "123?", -1.0D, true).map(LynxValue(_)))
    val map: Map[String, LynxList] = Map("lynxList" -> lynxList)
    val lynxMap: LynxMap = LynxMap(map)
    _testFunc(lynxMap)
  }

  //Test LynxMap[String, NestedLynxList]
  @Test
  def test16(): Unit = {
    val lynxList = LynxList(List(1, 2.0, "", "123?", -1.0D, true).map(LynxValue(_)))
    val nestedLynxList = LynxList(List(lynxList, LynxValue("test str"), LynxValue(0.1), LynxValue(true)))
    val map: Map[String, LynxList] = Map("nestedLynxList" -> nestedLynxList)
    val lynxMap: LynxMap = LynxMap(map)
    _testFunc(lynxMap)
  }

  //Test LynxNode
  @Test
  def test17(): Unit = {
    val lynxList = LynxList(List(1, 2.0, "", "123?", -1.0D, true).map(LynxValue(_)))
    val node = new TestLynxNode(100l, List("label1", "label2"), Map("p1" -> lynxList))
    _testFunc(node)
  }

  //Test LynxRelationship
  @Test
  def test18(): Unit = {
    val lynxList = LynxList(List(1, 2.0, "", "123?", -1.0D, true).map(LynxValue(_)))
    val relationship = new TestLynxRelationship(100l, "type1", 101l, 102l, Map("p1" -> lynxList))
    _testFunc(relationship)
  }

}
