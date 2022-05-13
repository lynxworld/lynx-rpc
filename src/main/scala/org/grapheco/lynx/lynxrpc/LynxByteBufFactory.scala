package org.grapheco.lynx.lynxrpc

import io.grpc.netty.shaded.io.netty.buffer.{ByteBuf, PooledByteBufAllocator}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:11 2022/4/15
 * @Modified By:
 */

// This object is to manage the ByteBuf.
object LynxByteBufFactory {
  val allocator: PooledByteBufAllocator = PooledByteBufAllocator.DEFAULT

  def getByteBuf: ByteBuf = allocator.directBuffer()

  def fromBytes(bytes: Array[Byte]): ByteBuf = {
    getByteBuf.writeBytes(bytes)
  }

  def releaseBuf(byteBuf: ByteBuf): Array[Byte] = {
    val dst = new Array[Byte](byteBuf.writerIndex())
    byteBuf.readBytes(dst)
    byteBuf.release()
    dst
  }

  def exportBuf(byteBuf: ByteBuf): Array[Byte] = {
    val dst = new Array[Byte](byteBuf.writerIndex())
    byteBuf.readBytes(dst)
    byteBuf.clear()
    dst
  }
}
