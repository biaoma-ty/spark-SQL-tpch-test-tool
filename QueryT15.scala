import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT15  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q15")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q15")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 15 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""drop view revenue_cached""")
    val res1 = sqlContext.sql("""drop view max_revenue_cached""")

    val res2 = sqlContext.sql("""create view revenue_cached as
                                  select l_suppkey as supplier_no,sum(l_extendedprice * (1 - l_discount)) as total_revenue
                                from lineitem
                                  where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'
                                group by l_suppkey""")

    val res3 = sqlContext.sql("""create view max_revenue_cached as
                                  select max(total_revenue) as max_revenue
                                from revenue_cached""")

    val res4 = sqlContext.sql("""select s_suppkey,s_name,s_address,s_phone,total_revenue
                                from supplier,revenue_cached,max_revenue_cached
                                where s_suppkey = supplier_no and total_revenue = max_revenue
                                order by s_suppkey""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 15 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)
      println(res1.queryExecution.executedPlan)
      println(res2.queryExecution.executedPlan)
      println(res3.queryExecution.executedPlan)
      println(res4.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res4.collect().foreach(println)
      }else{
        res4.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 15's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 15 completed!")

    sc.stop()
    println("ID: "+ID+"Query 15's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
  }

}









