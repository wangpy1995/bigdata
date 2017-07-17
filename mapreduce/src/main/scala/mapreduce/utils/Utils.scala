package mapreduce.utils

import java.io.IOException

import org.apache.spark.internal.Logging

import scala.util.control.NonFatal

/**
 * Various utility methods used by Ude.Copy from spark Utils.scala
 */
object Utils extends Logging{
    /**
     * Get the ClassLoader which loaded Spark.
     */
    def getSparkClassLoader: ClassLoader = getClass.getClassLoader

    /**
     * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
     * loaded Spark.
     *
     * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
     * active loader when setting up ClassLoader delegation chains.
     */
    def getContextOrSparkClassLoader: ClassLoader =
        Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

    /** Preferred alternative to Class.forName(className) */
    def classForName(className: String): Class[_] = {
        Class.forName(className, true, getContextOrSparkClassLoader)
        // scalastyle:on classforname
    }

    /**
     * build stack string
     *
     * @param stackTrace stackTrace element
     * @return stackTrace string
     */
    def stackTraceString(stackTrace: Array[StackTraceElement]): String = {
        val st = if (stackTrace == null) "" else stackTrace.map("        " + _).mkString("\n")
        st
    }

    def tryOrIOException[T](block: => T): T = {
        try {
            block
        } catch {
            case e: IOException =>
                logError("Exception encountered", e)
                throw e
            case NonFatal(e) =>
                logError("Exception encountered", e)
                throw new IOException(e)
        }
    }
}
