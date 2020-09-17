package com.atguigu.member.dao

import org.apache.spark.sql.SparkSession

object DwdMemberDao {

  def getDwdMember(sparkSession:SparkSession)={
    sparkSession.sql(
      """
        |SELECT
        |uid, ad_id, email, fullname, iconurl,
        |lastlogin, mailaddr, memberlevel, password, phone,
        |qq, register, regupdatetime, unitname, userip,
        |zipcode, dt, dn
        |FROM dwd.dwd_member
      """.stripMargin)
  }
  def getDwdMemberRegType(sparkSession: SparkSession) = {
    sparkSession.sql(
      """
        |SELECT
        |uid, appkey, appregurl, bdp_uuid,
        |createtime AS reg_createtime, isranreg,
        |regsource, regsourcename, websiteid AS siteid, dn
        |FROM dwd.dwd_member_regtype
      """.stripMargin)
  }

  def getDwdBaseAd(sparkSession: SparkSession) = {
    sparkSession.sql(
      """
        |SELECT
        |adid AS ad_id, adname, dn
        |FROM dwd.dwd_base_ad
      """.stripMargin)
  }

  def getDwdBaseWebSite(sparkSession: SparkSession) = {
    sparkSession.sql(
      """
        |SELECT
        |siteid,sitename,siteurl,delete as site_delete,
        |createtime as site_createtime,creator as site_creator,dn
        |FROM dwd.dwd_base_website
      """.stripMargin)
  }

  def getDwdVipLevel(sparkSession: SparkSession) = {
    sparkSession.sql(
      """
        |SELECT
        |vip_id, vip_level, start_time AS vip_start_time, end_time AS vip_end_time,
        |last_modify_time AS vip_last_modify_time, max_free AS vip_max_free, min_free AS vip_min_free,
        |next_level AS vip_next_level, operator AS vip_operator, dn
        |FROM dwd.dwd_vip_level
      """.stripMargin)
  }

  def getDwdPcentermemPayMoney(sparkSession: SparkSession) = {
    sparkSession.sql(
      """
        |SELECT
        |uid, CAST(paymoney AS decimal(10, 4)) AS paymoney, vip_id, dn
        |FROM dwd.dwd_pcentermempaymoney
      """.stripMargin)
  }
}
