package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * dws.dws_qz_chapte :
  * 4张表join
  * dwd.dwd_qz_chapter
  * inner join
  * dwd.qz_chapter_list  join条件：chapterlistid和dn ，
  * inner join
  * dwd.dwd_qz_point  join条件：chapterid和dn,
  * inner join
  * dwd.dwd_qz_point_question   join条件：pointid和dn
  */
object QzChapterDao {
  /**
    * 查询qz_chapter基础数据
    * @param sparkSession
    * @param dt
    */
  def getDwdQzChapter(sparkSession:SparkSession,dt:String)={
    sparkSession.sql(
      s"""
        |select
        |  chapterid,
        |  chapterlistid,
        |  chaptername,
        |  sequence,
        |  showstatus,
        |  creator as chapter_creator,
        |  createtime as chapter_createtime,
        |  courseid as chapter_courseid,
        |  chapternum,
        |  outchapterid,
        |  dt,
        |  dn
        |from
        |  dwd.dwd_qz_chapter
        |where
        |  dt='$dt'
      """.stripMargin)
  }

  /**
    * 查询dwd.qz_chapter_list基础数据
    * @param sparkSession
    * @param dt
    */
  def getDwdQzChapterList(sparkSession: SparkSession,dt: String)={
    sparkSession.sql(
      s"""
        |select
        |  chapterlistid,
        |  chapterlistname,
        |  chapterallnum,
        |  dn
        |from
        |  dwd.dwd_qz_chapter_list
        |where
        |  dt='$dt'
      """.stripMargin)
  }

  /**
    * 查询dwd.dwd_qz_point基础数据
    * @param sparkSession
    * @param dt
    */
  def getDwdQzPoint(sparkSession: SparkSession,dt:String)={
    sparkSession.sql(
      s"""
        |select
        |  pointid,
        |  pointname,
        |  pointyear,
        |  chapter,
        |  excisenum,
        |  pointlistid,
        |  chapterid,
        |  pointdescribe,
        |  pointlevel,
        |  typelist,
        |  score as point_score,
        |  thought,
        |  remid,
        |  pointnamelist,
        |  typelistids,
        |  pointlist,
        |  dn
        |from
        |  dwd.dwd_qz_point
        |where
        |  dt='$dt'
      """.stripMargin)
  }

  /**
    * 查询dwd.dwd_qz_point_question基础数据
    * @param sparkSession
    * @param dt
    */
  def getDwdQzPointQuestion(sparkSession: SparkSession,dt:String)={
    sparkSession.sql(
      s"""
        |select
        |  pointid,
        |  questionid,
        |  questype,
        |  dn
        |from
        |  dwd.dwd_qz_point_question
        |where
        |  dt='$dt'
      """.stripMargin)
  }
}
