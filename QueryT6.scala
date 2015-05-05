import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT6  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

  val conf = new SparkConf()

  conf.setAppName("TPCH-Q6")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q6")
    println("ID: "+ID+"query 6 will be submitted")
    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 6 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)
    //the query
    val res0 = sqlContext.sql("""select sum(l_extendedprice * l_discount) as revenue
                                from lineitem where l_shipdate >= '1993-01-01' and l_shipdate < '1994-01-01' and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 25""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 6 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")
    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{

      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }

      println("ID: "+ID+"query 6 will be executed")

      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 6's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 6 completed!")

    sc.stop()
    println("ID: "+ID+"Query 6's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }

}