package com.mob.dpi.jobs.bean

case class FileInfoBo(loadDay: String, source: String, modelType: String, day: String,
                      absFilePath: String, fileType: String,
                      taskId: String, taskName: String, taskConf: String, userName: String, isStart: String
                     )
