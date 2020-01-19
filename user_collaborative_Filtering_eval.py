#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pyspark import SparkContext
import os
os.environ['PYTHONHASHSEED'] = "66"
sc = SparkContext("local","ALS_ZD1")
sc.setLogLevel('WARN')
#
import numpy as np
from datetime import timedelta
from pyspark.sql import SparkSession

def combinepartition(a, b):
    a.extend(b)
    return a


# 处理数据，用户序号，资讯，得分
def get_data(datas, user_label = False):
    # data.createOrReplaceTempView('data')
    # datas = spark.sql('select * from data order by update_time desc limit 5000')
    # user_news_score,用户资讯得分表（user_id、资讯、得分、member_id）(user_id和member_id有一个为0)
    data_user = datas.select(datas.user_id.alias('user'), datas.news_id.alias('item'), datas.score.alias('rating'),datas.member_id.alias('member'))
    # x[0] is user_id, x[3] is member_id,x[1] is news_id ,x[2] is score
    data_user_collect = data_user.rdd.map(lambda x: (str(x[0]) + " " + str(x[3]) + " " + str(x[1]), [x[2]])).reduceByKey(combinepartition)
    # x[0]="user_id member_id item" x[1]=ave(rating)
    data_user_collect_merge = data_user_collect.map(lambda x: (x[0], np.mean(x[1]) if len(x[1]) > 1 else x[1][0]))
    # 用户标签（"user_id member_id"，用户序号）,将用户标签序列化，便于预测
    if user_label == False:  # 训练时候
        user_label = data_user.rdd.map(lambda x: str(x[0]) + " " + str(x[3])).distinct().zipWithIndex()
        print(user_label.take(10))
    # [('0 427722', 0), ('16310446 0', 1), ('0 500374', 2), ('0 726450', 3), ('0 902359', 4), ('0 900474', 5), ('0 905028', 6), ('0 904945', 7), ('16314037 0', 8), ('0 875058', 9)]
    user_data_join = data_user_collect_merge.map(lambda x: (x[0].split(" ")[0] + " " + x[0].split(" ")[1], (x[0].split(" ")[-1], x[1]))).join(user_label).map(lambda x: (x[1][1], int(x[1][0][0]), x[1][0][1]))
    print(user_data_join.take(10))
    return user_data_join, user_label

if __name__ == "__main__":

    # 连接数据库
    RECOMMEND_JDBC = 'jdbc:mysql://192.168.1.158:3306/chenz'
    JDBC_USER = 'root'
    JDBC_PASSWORD = '123456'

    spark = SparkSession.builder.appName('user_coll').config('spark.default.parallelism','10').getOrCreate()
    # file_name = os.listdir(os.getcwd())
    # spark.sparkContext.setCheckpointDir(os.getcwd())
    # user_news_score,用户资讯得分表
    data = spark.read.jdbc(url=RECOMMEND_JDBC,table='news_score',properties={'user':JDBC_USER,'password':JDBC_PASSWORD})
    # x[0] is add_time
    # 用了7天的数据做协同过滤，时间可能有点短，数据少预测中会出现较多负值
    max_time = data.select(data.add_time).rdd.map(lambda x: x[0] - timedelta(days=7)).max()
    datas = data.filter(data.add_time > max_time)
    if datas.count() > 2:
        # 处理数据，用户序号，资讯，得分
        user_data_join, user_label = get_data(datas)

        # ALS评估
        from pyspark.mllib.recommendation import Rating, ALS
        # 1.训练数据（用户序号,item,rating）
        ratings = user_data_join.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
        print(ratings.take(5))
        # [Rating(user=0, product=8906859, rating=5.0), Rating(user=0, product=8870268, rating=2.0), Rating(user=1, product=8859878, rating=5.0), Rating(user=2, product=8841222, rating=2.0), Rating(user=2, product=8903396, rating=1.32)]

        model = ALS.train(ratings,50,10,0.01)
        user_features = model.userFeatures()

        #2.测试单个用户，用户user推荐top10的items，其items顺序为降序
        user = 3
        topKRecs = model.recommendProducts(user, 10)
        for i in topKRecs:
            print(i)

        #比对user用户所评级过得电影  和   被推荐的电影
        movies_for_user = ratings.groupBy(lambda x:x.user).mapValues(list).lookup(user)
        print(movies_for_user)
        print('user用户对%d部电影进行了评级'%len(movies_for_user[0]))
        print('源数据中用户(userId=user)喜欢的电影(item)：')
        for i in sorted(movies_for_user[0],key=lambda x : x.rating,reverse=True):
            print(i.product)
        # Rating(user=3, product=8841181, rating=4.001524222963966)
        # Rating(user=3, product=8885338, rating=3.9424299366362137)
        # Rating(user=3, product=8849919, rating=3.024131035229358)
        # Rating(user=3, product=8906981, rating=2.6830711876237374)
        # Rating(user=3, product=8860544, rating=2.614969684611492)
        # Rating(user=3, product=8884968, rating=2.4056717139505404)
        # Rating(user=3, product=8819095, rating=2.4020681950603837)
        # Rating(user=3, product=8885288, rating=2.2546903053245835)
        # Rating(user=3, product=8859878, rating=2.17985201377886)
        # Rating(user=3, product=8824499, rating=2.153274318393534)

        actual = movies_for_user[0][2]
        actualRating = actual.rating
        print ('用户user对电影1012的实际评级',actualRating)
        predictedRating = model.predict(user, actual.product)
        print('用户user对电影1012的预测评级',predictedRating)
        squaredError = np.power(actualRating-predictedRating,2)
        print('user实际评级与预测评级的MSE',squaredError)

        actualMovies = [rating.product for rating in movies_for_user[0]]
        predictMovies = [rating.product for rating in topKRecs]
        print('用户user实际的电影：',actualMovies)
        print('用户user预测的电影：',predictMovies)
        # 用户user实际的电影： [8901142, 8874928, 8903486, 8885338, 8824811, 8825964, 8841181, 8823957, 8849919, 8860147, 8644053]
        # 用户user预测的电影： [8841181, 8885338, 8849919, 8906981, 8860544, 8884968, 8819095, 8885288, 8859878, 8824499]

        #3.整体实际与预测数据评估
        # 测试数据
        min_time = max_time - timedelta(days=1)
        print(max_time, min_time)
        datas = data.filter(data.add_time < max_time).filter(data.add_time > min_time)

        # 处理数据，用户序号，资讯，得分
        user_data_join, user_label = get_data(datas, user_label)

        # ALS评估
        from pyspark.mllib.recommendation import Rating, ALS

        # 测试数据（用户序号,item,rating）
        ratings = user_data_join.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))

        userProducts = ratings.map(lambda rating:(rating.user,rating.product))
        print('实际的评分电影:',userProducts.take(5))
        # print (model.predictAll(userProducts).collect()[0])
        predictions = model.predictAll(userProducts).map(lambda rating:((rating.user,rating.product), rating.rating))
        print('预测的评分:',predictions.take(5))
        
        ratingsAndPredictions = ratings.map(lambda rating:((rating.user,rating.product),rating.rating)).join(predictions)
        print('组合预测的评分和实际的评分:',ratingsAndPredictions.take(5))
        # 组合预测的评分和实际的评分: [((1730, 8904080), (1.52, 1.5183552691178965)), ((2634, 8903648), (1.08, 1.109481704984579)), ((412, 8824284), (1.94, 1.9368518512651538)), ((759, 8841175), (2.0, 1.9916580018716354)),((846, 8825136), (5.0, 4.987908502485232))]

        from pyspark.mllib.evaluation import RegressionMetrics
        from pyspark.mllib.evaluation import RankingMetrics
        #((196, 242), (3.0, 3.089619902353484))
        predictedAndTrue = ratingsAndPredictions.map(lambda x:x[1][0:2])
        print(predictedAndTrue.take(5))
        regressionMetrics = RegressionMetrics(predictedAndTrue)
        print("均方误差 = %f"%regressionMetrics.meanSquaredError)
        print("均方根误差 = %f"% regressionMetrics.rootMeanSquaredError)
        # 均方误差 = 0.000245
        # 均方根误差 = 0.015641