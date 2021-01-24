/*
From the below file print the unique item and store name:

retailer.txt:

Sears,shoe,ring,pan,shirt
Walmart,ring,pan,hat,meat
Target,shoe,pan,shirt,hat

*/

Code:

>spark-shell
scala>var uniqueItem=""
scala> var storeName=""
scala> val rdd1 = sc.textFile("file://home/user1/retailer.txt")
scala> rdd1.map(x=>x.split(",")).map(x=>Array(x(1),x(2),x(3),x(4))).flatMap(x=>x).map(x=>(x,1)).reduceByKey(_+_).collect().foreach(x=>if(x._2==1)uniqueItem=x._1)
scala> rdd1.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4))).collect().foreach(x=>if((x._2==uniqueItem)||(x._3==uniqueItem)||(x._4==uniqueItem)||(x._5==uniqueItem))storeName=x._1)
scala> println("Store Name " + storeName)
scala> println("Unique Item " + uniqueItem)

Output:
Store Name Walmart
UNIQUE ITEM meat
