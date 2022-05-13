package org.grapheco.lynx.lynxrpc

import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.LynxId


/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:20 2022/4/18
 * @Modified By:
 */
class LynxIdImpl(lynxIdValue: Any) extends LynxId{
  override val value: Any = lynxIdValue

  override def toLynxInteger: LynxInteger = value match {
    case lynxInteger: LynxInteger => lynxInteger
    case long: Long => LynxInteger(long)
    case int: Int => LynxInteger(int)
    case _ => throw new Exception("Unexpected LynxId Value.")
  }
}
