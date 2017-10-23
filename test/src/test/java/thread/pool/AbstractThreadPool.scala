package thread.pool

import java.util.concurrent._

import test.pool.TaskState
import thread.task.FTask

class AbstractThreadPool(corePoolSize: Int,
                         maximumPoolSize: Int,
                         keepAliveTime: Long,
                         unit: TimeUnit,
                         workQueue: BlockingQueue[Runnable])
  extends ThreadPoolExecutor(corePoolSize,
    maximumPoolSize,
    keepAliveTime,
    unit, workQueue) {

  val resultQueue = new LinkedBlockingQueue[TaskResult]()
  val waitingQueue = new LinkedBlockingQueue[Runnable]()

  override def beforeExecute(t: Thread, r: Runnable): Unit = super.beforeExecute(t, r)

  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    super.afterExecute(r, t)

  }

  override def submit(task: Runnable): Future[_] = {
    val t = task.asInstanceOf[FTask]
    t.state = TaskState.STARTED
    this.resultQueue.remove(t)
    super.submit(t)

    val result = TaskResult(t.taskId, TaskState.WAITING)
    resultQueue.put(result)
    new FutureTask[TaskResult](t, result)
  }
}


case class TaskResult(code: Int, status: TaskState)
