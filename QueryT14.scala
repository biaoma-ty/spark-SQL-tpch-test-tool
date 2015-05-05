import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT14  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q14")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)


  override def execute: Unit ={

//    setAppName("TPC-H_Q14")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 14 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
                                from lineitem,part
                                where l_partkey = p_partkey and l_shipdate >= '1995-08-01'and l_shipdate < '1995-09-01'""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 14 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 14's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 14 completed!")

    sc.stop()
    println("ID: "+ID+"Query 14's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
    "OK"
  }

}
