package base.data.loader

trait Loader {
  type V

  def doLoad(): V

  def loadAs[T]():T
}

trait Converter {
  type In
  type Out
  def convert[U>:Out](src: In): U
}
