import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT3  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

  val conf = new SparkConf()

  conf.setAppName("TPCH-Q3")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 3 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""select l_orderkey,sum(l_extendedprice * (1 - l_discount)) as revenue,o_orderdate,o_shippriority
                                from customer,orders,lineitem
                                where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-22' and l_shipdate > '1995-03-22'
                                group by l_orderkey,o_orderdate,o_shippriority
                                order by revenue desc,o_orderdate
                                limit 10""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 3 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")
    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{

      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }

      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 3's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 3 completed!")

    sc.stop()
    println("ID: "+ID+"Query 3's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }

}
