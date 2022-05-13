import org.grapheco.lynx.lynxrpc.LynxIdImpl
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 08:33 2022/4/18
 * @Modified By:
 */
class TestLynxNode(nid: Long, nlabels: Seq[String], props: Map[String, LynxValue]) extends LynxNode{
  override val id: LynxId = new LynxIdImpl(nid)

  override def labels: Seq[LynxNodeLabel] = nlabels.map(string => LynxNodeLabel(string))

  override def keys: Seq[LynxPropertyKey] = props.keys.map(key => LynxPropertyKey(key)).toSeq

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey.value)
}
