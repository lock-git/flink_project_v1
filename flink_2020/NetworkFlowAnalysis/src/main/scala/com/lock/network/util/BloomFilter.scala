package com.lock.network.util

import java.util

import scala.util.hashing.MurmurHash3

/**
  * author  Lock.xia
  * Date 2020-12-30
  */


/**
  * 实现简单的bloomfilter过滤器，用来判断某个用户是否存在
  */
object BloomFilter {

  /** 1 << 24位长度的位图数组，存放hash值 */
  val bitSetSize: Int = 1 << 30

  /** 位数组 */
  val bitSet = new util.BitSet(bitSetSize)

  /** 传入murmurhash中的seed的范围 */
  val seedNums = 6

  /**
    * 根据MurmurHash3计算哈希值，设置BitSet的值
    *
    * @param str
    */
  def hashValue(str: String): Unit = {
    if (str != null && !str.isEmpty)
      for (i <- 1 to seedNums) bitSet.set(Math.abs(MurmurHash3.stringHash(str, i)) % bitSetSize, true)
    else
      println("传入的字符串位空")
  }

  /**
    * 判断一个字符串是否存在于bloomFilter
    *
    * @param str
    * @return
    */
  def exists(str: String): Boolean = {
    def existsRecur(str: String, seed: Int): Boolean = {
      if (str == null || str.isEmpty) false
      else if (seed > seedNums) true
      else if (!bitSet.get(Math.abs(MurmurHash3.stringHash(str, seed)) % bitSetSize)) false
      else existsRecur(str, seed + 1)
    }

    if (str == null || str.isEmpty)
      false
    else
      existsRecur(str, 127)
  }

  def main(args: Array[String]): Unit = {
    val s1 = "www.baidu.com"
    val s2 = "www.jd.com"
    val s3 = "www.taobao.com"
    val s4 = "http://kafka.apache.org"
    BloomFilter.hashValue(s1)
    BloomFilter.hashValue(s2)
    BloomFilter.hashValue(s3)
    BloomFilter.hashValue(s4)
    println(BloomFilter.exists(s1))
    println(BloomFilter.exists(s2))
    println(BloomFilter.exists(s3))
    println(BloomFilter.exists(s4))
  }

}
