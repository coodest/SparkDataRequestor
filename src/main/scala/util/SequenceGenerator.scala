package util

import java.util.Random

import scala.collection.mutable.ListBuffer

/**
  * Created by worker on 2016/7/11.
  */
class SequenceGenerator {
  def uniformRandSeq(start: Int, end: Int, number: Int): Array[Int] = {
    val seq: Array[Int] = new Array[Int](number)
    if (start == end) {
      seq(0) = start
    }
    else {
      val random: Random = new Random
      var i: Int = 0
      while (i < number) {
        seq(i) = random.nextInt(end - start + 1) + start
        var j: Int = 0
        while (j < i) {
          if (seq(j) == seq(i)) {
            i -= 1
          }

          j += 1
        }

        i += 1
      }
    }
    return seq
  }

  def haltonSeq(number: Int, bases: Array[Int]): ListBuffer[Array[Double]] = {
    val seq: ListBuffer[Array[Double]] = new ListBuffer[Array[Double]]
    val folds: Array[Array[Double]] = new Array[Array[Double]](bases.length)

    var i: Int = 0
    while (i < folds.length) {
      folds(i) = new Array[Double](number)

      i += 1
    }

    i = 0
    while (i < bases.length) {
      val dividend: Double = 1.0 / bases(i)
      var j: Int = 0
      while (j < number) {
        folds(i)(j) = if (j == 0) dividend
        else folds(i)(j - 1) * dividend

        j += 1
      }

      i += 1
    }

    i = 1
    while (i <= number) {
      val result: Array[Double] = new Array[Double](bases.length)
      var j: Int = 0
      while (j < bases.length) {
        var num: Int = i
        var k: Int = 0
        while (num != 0) {
          result(j) += num % bases(j) * folds(j)(k)
          num /= bases(j)

          k += 1
        }

        j += 1
      }
      seq += result

      i += 1
    }
    return seq
  }
}
