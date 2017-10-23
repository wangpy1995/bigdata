package thread.task

import test.pool.TaskState
import thread.pool.AbstractThreadPool

class FTask(val taskId: Int,
            val isTask: Boolean,
            var state: TaskState,
            var finished: Boolean,
            val threadPool: AbstractThreadPool,
            func: () => Unit) extends Runnable {

  def beforeRun() = {
    finished = false
    state = TaskState.STARTED
    threadPool.waitingQueue.remove(this)
    threadPool.resultQueue
  }

  def afterRun() = {
    finished = true
    if (state != TaskState.KILLED){}
  }

  def notifyTaskStates(task: FTask): Unit = {

  }

  override def run(): Unit = {
    try {
      beforeRun()
      func()
      afterRun()
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

//  def
}
