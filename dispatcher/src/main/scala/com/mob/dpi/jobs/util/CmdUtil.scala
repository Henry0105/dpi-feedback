package com.mob.dpi.jobs.util

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.util.concurrent.TimeUnit


object CmdUtil {

  // 获取输出
  private def printMessage(input: InputStream): Unit = {

    new Thread(new Runnable {

      override def run(): Unit = {

        val in = new BufferedReader(new InputStreamReader(input))
        var line: String = in.readLine()

        try {
          while (line != null && !line.equalsIgnoreCase("null")) {
            println(line)
            line = in.readLine()
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          in.close()
        }
      }
    }).start()
  }

  def runCmd(cmd: String, timeout: Long): Int = {

    val runtime = Runtime.getRuntime()
    var proc1: Process = null
    // val cmd = "sqoop list-databases --connect jdbc:mysql://10.21.33.28:3306
    // --username root --password mobtech2019java"

    try {
      proc1 = runtime.exec(cmd)

      printMessage(proc1.getInputStream)
      printMessage(proc1.getErrorStream)

      proc1.waitFor(timeout, TimeUnit.SECONDS)

      val res = proc1.exitValue()
      if (res == 0) {
        println("run cmd success")
      } else {
        println("run cmd faile")
      }

      res
    } catch {
      case e1: IOException =>
        e1.printStackTrace()
        throw e1
      case e2: InterruptedException =>
        e2.printStackTrace()
        throw e2
    } finally {
      proc1.destroy()
      proc1 = null
    }
  }

  def runCmds(cmds: Array[String], timeout: Long): Int = {

    val runtime = Runtime.getRuntime()
    var proc1: Process = null
    // val cmd = "sqoop list-databases --connect jdbc:mysql://10.21.33.28:3306
    // --username root --password mobtech2019java"

    try {
      proc1 = runtime.exec(cmds)

      printMessage(proc1.getInputStream)
      printMessage(proc1.getErrorStream)

      proc1.waitFor(timeout, TimeUnit.SECONDS)

      val res = proc1.exitValue()
      if (res == 0) {
        println("run cmd success")
      } else {
        println("run cmd faile")
      }

      res
    } catch {
      case e1: IOException =>
        e1.printStackTrace()
        throw e1
      case e2: InterruptedException =>
        e2.printStackTrace()
        throw e2
    } finally {
      proc1.destroy()
      proc1 = null
    }
  }


  def main(args: Array[String]): Unit = {

    val res = runCmd(args(0), 10)
    //    println("---------------------------")
    //    runCmds(args, 10)

    if (res == 0) {
      println("do.")
    } else {
      println("skip.")
    }

  }
}
