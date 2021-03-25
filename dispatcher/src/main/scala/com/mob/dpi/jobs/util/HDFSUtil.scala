package com.mob.dpi.jobs.util

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSUtil {

  def using[Closable <: {def close(): Unit}, T](c: Closable)(f: Closable => T): T =
    try {
      f(c)
    } finally {
      c.close()
    }

  def hdfs_exists(path: String): Boolean = {
    using(FileSystem.get(new Configuration())) {
      sys => {
        sys.exists(new Path(path))
      }
    }
  }


  def hdfs_ls(path: String): List[(Long, String)] = {
    using(FileSystem.get(new Configuration())) {
      sys =>
        val files = sys.listStatus(new Path(path))
        files.map(f => {
          (f.getLen, f.getPath.toString)
        }).toList
    }
  }

  def hdfs_ls_r(sys: FileSystem, path: Path, deep: Int): List[(Long, String)] = {
    sys.listStatus(path)
      .flatMap(f => {
        if (deep > 1 && f.isDirectory) {
          hdfs_ls_r(sys, f.getPath, deep - 1)
        } else {
          List((f.getLen, f.getPath.toString))
        }
      }).toList
  }

  def hdfs_lsr(path: String, deep: Int = 4): List[(Long, String)] = {


    using(FileSystem.get(new Configuration())) {
      sys => {
        sys
          .listStatus(new Path(path))
          .flatMap(f => {
            if (deep > 1 && f.isDirectory) {
              hdfs_ls_r(sys, f.getPath, deep - 1)
            } else {
              List((f.getLen, f.getPath.toString))
            }
          }).toList
      }
    }
  }

//  def hdfs_lsr_file(path: String, deep: Int = 4): List[(Long, String)] = {
//    using(FileSystem.get(new Configuration())) {
//      sys => {
//        hdfs_lsr(path,deep).filter(kv => sys.getFileStatus(new Path(kv._2)).isFile)
//      }
//    }
//  }


  def hdfs_lsr_filter(path: String, deep: Int = 4, f: String => Boolean): List[(Long, String)] = {
    hdfs_lsr(path,deep).filter(tpl => f(tpl._2))
  }

  def hdfs_ls_test(path: String): Unit = {
    using(FileSystem.get(new Configuration())) {
      sys => {
        sys.listStatus(new Path(path)).map(f => {
          println(f.getGroup)
          println(f.getOwner)
          println(f.getModificationTime)
          println(f.getAccessTime)
          println(f.getLen)
          println(f.getPath.getName)
          println(f.getPath.toString)
        })
      }
    }
  }

  def main(args: Array[String]): Unit = {

    HDFSUtil.hdfs_ls(args(0)).map(f => {
      println(f._1, f._2)
    })

    println("--------------------------")

    HDFSUtil.hdfs_ls_test(args(0))


    println("--------------------------")

    HDFSUtil.hdfs_lsr(args(0)).map(f => {
      println(f._1, f._2)
    })

    println("----------filter----------------")

    HDFSUtil.hdfs_lsr_filter(args(0), 5, f => f.split("/").last.startsWith("txt.") && f.endsWith(".txt")).map(f => {
      println(f._1, f._2)
    })


    print("+++++++++++++++++++++++++++++++++++++++")
    if (HDFSUtil.hdfs_exists(args(0))) {
      println("file exists")
    } else {
      println("file no exists")
    }


    println("------------------------------------")
//    hdfs_lsr_file(args(0))
  }
}
