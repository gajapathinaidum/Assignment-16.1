package com.spark.core.prctice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkAssignment1 extends App{


  val listVal=List[Int](1,2,3,4,5,6,7,8,9,10)
  val conf=new SparkConf().setMaster("local").setAppName("SparkAssignment1")
  val sc=new SparkContext(conf)
  val Rdd1 = sc.parallelize(listVal)
   //1.find sum of all numbers
  var sum= Rdd1.sum
  println(sum);
  //2.find total elements in list
  println(Rdd1.count)
  
     //3.calculate average of numbers
   println((sum:Double)/Rdd1.count)
   //4.find sum of all even numbers
   var Rdd2=Rdd1.filter(f=>f%2==0)
   println(Rdd2.sum);
  //5 Find the total numbers of list divisible by both 5 and 3
     var Rdd3 =Rdd1.filter(a=>(a%3==0 && a%5==0))
     print(Rdd3.foreach(println))
}