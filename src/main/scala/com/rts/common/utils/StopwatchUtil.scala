package com.rts.common.utils

/**
  * Created by tangning on 2018/1/23.
  * 计时器
  */
class StopwatchUtil {
  private val start = System.currentTimeMillis()

  override def toString() = (System.currentTimeMillis() - start) + " ms"
}
