package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

object QzPaperDao {
  /**
    * dws.dws_qz_paper(试卷维度表)
    * dws.dws_qz_paper:
    * 4张表join
    * qz_paperview
    * left join
    * qz_center join 条件：paperviewid和dn,
    * left join
    * qz_center  join 条件：centerid和dn,
    * inner join
    * qz_paper join条件：paperid和dn
    */
  def getDwdQzPaperView(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest," +
      "contesttime,conteststarttime,contestendtime,contesttimelimit,dayiid,status,creator as paper_view_creator," +
      "createtime as paper_view_createtime,paperviewcatid,modifystatus,description,papertype,downurl,paperuse," +
      s"paperdifficult,testreport,paperuseshow,dt,dn from dwd.dwd_qz_paper_view where dt='$dt'")
  }

  def getDwdQzCenterPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select paperviewid,sequence,centerid,dn from dwd.dwd_qz_center_paper where dt='$dt'")
  }

  def getDwdQzPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperid,papercatid,courseid,paperyear,chapter,suitnum,papername,totalscore,chapterid," +
      s"chapterlistid,dn from dwd.dwd_qz_paper where dt='$dt'")
  }

  def getDwdQzCenter(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select centerid,centername,centeryear,centertype,centerparam,provideuser," +
      s"centerviewtype,stage,dn from dwd.dwd_qz_center where dt='$dt'")
  }
}
