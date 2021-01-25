/*
marks.csv (rollno, sub1,sub2,sub3,sub4,sub5,sub6)
1001,76,72,85,75,84,75
1002,77,51,90,61,76,69
1003,91,86,52,81,64,87
1004,71,82,59,96,82,73
1005,97,52,72,49,85,64
1006,82,80,71,82,83,69
1007,67,93,93,54,61,56
1008,81,73,82,70,85,74
1009,63,89,81,73,88,60
1010,73,66,70,69,77,68
1011,70,84,55,86,66,72
1012,73,85,85,53,65,56
1013,93,94,61,55,62,58
1014,69,95,73,58,72,89
1015,87,71,86,85,65,60
1016,90,59,77,90,66,55
1017,72,51,84,80,71,77
1018,94,86,59,66,83,56
1019,79,90,56,83,76,56
*/

copy marks.csv to HDFS

 hdfs dfs -put marks.csv sparkdata/

load it as RDD and calculate the percentage of each student and save it in HDFS

>spark-shell
scala> val marksrdd = sc.textFile("hdfs://localhost:54310/user/hduser/sparkdata/marks.csv")
scala> val markspairedrdd = marksrdd.map(x=>(x.split(",")(0),List(x.split(",")(1).toFloat,x.split(",")(2).toFloat,x.split(",")(3).toFloat,x.split(",")(4).toFloat,x.split(",")(5).toFloat,x.split(",")(6).toFloat)))
scala> val calculate_percentage = markpairedrdd.mapValues(x=>x.sum/x.length)
scala> calculate_percentage.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkdata/students_percentage.csv")
scala>:q
