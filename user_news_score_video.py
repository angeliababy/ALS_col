# coding=utf-8

# 计算用户对小视频的得分
'''
用户资讯得分方案：
1. 如果不是有效阅读（阅读时长<2秒），得分为0
2. 阅读，直接2分*阅读占比
3. 点赞，直接3分
4. 评论，直接4分
5. 收藏或者分享，直接5分
6. 不喜欢，直接-1分
'''

# spark-submit --master local --jars /home/spider/code/reconmmend_test/Trunk/news_recommed_reckon_java/action_reckon/searchword/lib/mysql-connector-java-5.1.38.jar --conf spark.pyspark.python=/usr/local/bin/python3 --conf spark.pyspark.driver.python=/usr/local/bin/python3 offline/user_news_score_t.py

'''
CREATE TABLE `news_read` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `user_id` int(11) NOT NULL DEFAULT '0',
  `member_id` int(11) NOT NULL DEFAULT '0',
  `channel_id` int(11) DEFAULT NULL COMMENT '频道编号',
  `news_id` int(11) DEFAULT NULL COMMENT '文章编号',
  `entry_datetime` datetime DEFAULT NULL COMMENT '进入时间',
  `leave_datetime` datetime DEFAULT NULL COMMENT '离开时间',
  `readed_percent` int(11) DEFAULT NULL COMMENT '阅读进度',
  `is_try_share` bit(1) DEFAULT b'0' COMMENT '是否尝试分享',
  `review_count` int(11) DEFAULT '0' COMMENT '评论数量',
  `is_review` bit(1) DEFAULT b'0' COMMENT '是否评论',
  `is_collect` bit(1) DEFAULT NULL COMMENT '是否收藏',
  `is_praise` bit(1) DEFAULT NULL COMMENT '是否点赞',
  `is_tread` bit(1) DEFAULT b'0' COMMENT '是否踩',
  `lang` int(11) DEFAULT '-1' COMMENT '1:中文、2:柬文、3:英文',
  `platform` int(11) DEFAULT '-1' COMMENT '1：安卓，2：苹果',
  `batch_no` varchar(64) NOT NULL DEFAULT '' COMMENT '批次号',
  `source` tinyint(4) NOT NULL DEFAULT '99' COMMENT '资讯阅读入口（1、历史 2、收藏 3、频道(遗弃) 4、相关推荐 5、搜索 6、用户动态  7、资讯模块 8、视频模块 9、小视频模块 99、其它）',
  `news_type` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '资讯类型（1、图文，2、图集，3、视频，4、小说、6、广告推广 8、小视频）',
  `add_datetime` datetime DEFAULT NULL COMMENT '添加时间',
  `app_version` varchar(128) DEFAULT '' COMMENT 'APP版本',
  `ip` varchar(255) DEFAULT '' COMMENT '客户端IP',
  `device_id` varchar(255) DEFAULT '' COMMENT '设备ID',
  `country` varchar(255) DEFAULT NULL,
  `province` varchar(255) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL,
  `uuid` varchar(128) DEFAULT '' COMMENT '客户端全局唯一id',
  `log_id` varchar(64) NOT NULL DEFAULT '' COMMENT '日志Id',
  `relation_news_ids` varchar(255) NOT NULL DEFAULT '' COMMENT '延展阅读曝光的资讯',
  PRIMARY KEY (`id`),
  KEY `news_id` (`news_id`,`channel_id`,`member_id`) USING BTREE,
  KEY `channel_id` (`channel_id`,`news_id`,`member_id`),
  KEY `add_datetime` (`add_datetime`),
  KEY `channelId_memberId_time` (`channel_id`,`member_id`,`add_datetime`),
  KEY `member_id` (`member_id`,`add_datetime`,`channel_id`) USING BTREE,
  KEY `user_id` (`user_id`,`add_datetime`,`channel_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=53485290 DEFAULT CHARSET=utf8mb4 COMMENT='资讯阅读行为表'
'''

import time
import arrow
from datetime import timedelta

from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import max

def scoredata(row_variable):
    new_variable = []
    #id
    new_variable.append(row_variable[12])
    #user(user后加1)
    uuid_and_account = []
    uuid_and_account.append(row_variable[0])
    uuid_and_account.append(row_variable[1])
    # print(uuid_and_account)
    new_variable.append(
        str(uuid_and_account[1]) + " " + "0" if uuid_and_account[1] != 0 else str(uuid_and_account[0]) + " " + "1")
    #news_id
    new_variable.append(row_variable[2])
    # time
    timetmp = time.mktime(row_variable[9].timetuple()) - time.mktime(row_variable[8].timetuple())
    new_variable.append(timetmp)

    if timetmp > 2:
        if row_variable[4] == 1 or row_variable[5] == 1:
            new_variable.append(5)
        elif row_variable[6] == 1:
            new_variable.append(4)
        elif row_variable[7] == 1:
            new_variable.append(3)
        else:
            score = round(row_variable[3]/100*2, 2)
            if score > 2:
                score = 2
            new_variable.append(score)
    else:
        new_variable.append(0)
    new_variable.append(row_variable[11])

    # print(new_variable)
    return new_variable


if __name__ == '__main__':

    # 连接数据库
    COLLECT_JDBC = '***'
    JDBC_USER = '***'
    JDBC_PASSWORD = '***'

    spark = SparkSession.builder.appName(name='news_read_score').getOrCreate()
    news_read = spark.read.jdbc(url=COLLECT_JDBC,table='news_read',numPartitions=100,properties={'user': JDBC_USER, 'password': JDBC_PASSWORD})
    news_read = news_read.select(news_read.user_id,news_read.member_id,news_read.news_id,news_read.readed_percent,news_read.is_try_share,news_read.is_collect,news_read.is_review,news_read.is_praise,news_read.entry_datetime,news_read.leave_datetime,news_read.news_type,news_read.add_datetime,news_read.id)

    # 处理时间片段（过滤数据，news_type = 8小视频）
    max_time = news_read.select(max(news_read.add_datetime)).rdd.map(lambda x: x[0] - timedelta(days=7)).collect()
    print(max_time)
    news_read = news_read.filter(news_read.add_datetime > max_time[0]).filter(news_read.news_type == 8)
    # print(news_read.take(20))

    # 选取数据并处理
    news_data = news_read.rdd.map(lambda x: scoredata(x)).filter(lambda x: x[3] > 2).filter(lambda x: x[1]!= "0 1")
    print(news_data.take(20))

    systime = arrow.now().format('YYYY-MM-DD HH:mm:ss')
    print(systime)
    # 用户资讯得分
    result_perent = news_data.map(lambda x: Row(id=x[0], user_id=int(x[1].split(" ")[0]) if int(x[1].split(" ")[1]) == 1 else 0,
                            member_id=int(x[1].split(" ")[0]) if int(x[1].split(" ")[1]) == 0 else 0, news_id=x[2], score=float(x[4]),add_time=x[5],update_time=str(systime)))
    # print(result_perent.take(20))

    result_perent = spark.createDataFrame(result_perent)

    # result_perent.createOrReplaceTempView("data")
    # spark.sql("select * from data").show()

    # 存储数据
    result_perent.write.jdbc(url=COLLECT_JDBC,table='news_score',mode="append",properties={'user': JDBC_USER, 'password': JDBC_PASSWORD, "driver":"com.mysql.cj.jdbc.Driver"})

    print("success!")

    spark.stop()



