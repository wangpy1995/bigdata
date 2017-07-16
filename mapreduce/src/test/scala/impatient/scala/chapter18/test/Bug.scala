package impatient.scala.chapter18.test

/**
  * Created by wpy on 17-7-1.
  */
class Bug(var x: Int = 0, var y: Int = 0) {
  var isTurned = false

  def move(step: Int): this.type = {
    if (isTurned) y += step
    else x += step
    this
  }

  def turn(x:this.type): this.type = {
    isTurned = !isTurned
    this
  }

  def show: this.type = {
    println((x, y))
    this
  }

  def and(x:this.type ):this.type = x
  def then:this.type =this
  def around:this.type =this
}

object TestBug {
  def main(args: Array[String]): Unit = {
    val bugsy = new Bug
//    bugsy.move(4).show.move(6).show.turn.move(5).show
    bugsy move 4 and bugsy.show and bugsy.then move 6 and bugsy.show turn bugsy.around move 5  and bugsy.show
  }
}
