package variance.test

/**
  * Created by wpy on 2017/6/23.
  */
object VariancePractice {
  def main(args: Array[String]): Unit = {
    //    val p = new Pair1("1", 100)
    val p = new Pair3("1", 2)
    println(s"before: $p")
    val after = p.swap(p)
    println(s"after: $after")
  }
}

trait SimpleString {
  val simpleString: Any => String = cls => s"$cls: ${cls.getClass.getSimpleName}"

  def toString(first: Any, second: Any): String = s"${this.getClass.getSimpleName}(${simpleString(first)}, ${simpleString(second)})"
}

//1不可便Pair[T,S] 使用swap产生新的交换过的对偶
class Pair1[T, S](first: T, second: S) extends SimpleString {
  def swap: Pair1[S, T] = new Pair1(second, first)

  override def toString: String = super.toString(first, second)
}

//可变类
class Pair2[T](var first: T, var second: T) extends SimpleString {
  def swap = {
    val temp = first
    first = second
    second = temp
  }

  override def toString: String = super.toString(first, second)
}

class Pair3[T, S](val first: T, val second: S) extends SimpleString {
  def swap(p: Pair3[T, S]) = new Pair3(p.second, p.first)

  override def toString: String = super.toString(first, second)
}


