package impatient.scala.chapter18.test

/**
  * Created by wpy on 17-7-2.
  */
trait IterableA[E, C[_]] {
  def iterator(): Iterator[E]

  def build[F](): C[F]

  def map[F](f: (E) => F): C[F]
}

class Buffer[E] extends IterableA[E,Buffer] {
  override def iterator(): Iterator[E] = ???

  override def build[F](): Buffer[F] = ???

  override def map[F](f: (E) => F): Buffer[F] = ???
}

trait Container[E]{
  def +=(e:E):Unit
}
