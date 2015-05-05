import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT4  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

  val conf = new SparkConf()

  conf.setAppName("TPCH-Q4")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q4")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 4 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""INSERT OVERWRITE TABLE q4_order_priority_tmp
                                  select
                                DISTINCT l_orderkey
                                  from
                                lineitem
                                where
                                l_commitdate < l_receiptdate""")

    val res1 = sqlContext.sql("""select o_orderpriority, count(1) as order_count
                                from orders o join q4_order_priority_tmp t
                                  on o.o_orderkey = t.o_orderkey and o.o_orderdate >= '1993-07-01' and o.o_orderdate < '1993-10-01'
                                group by o_orderpriority
                                order by o_orderpriority""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 4 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)
      println(res1.queryExecution.executedPlan)

    }else{

      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }

      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 4's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 4 completed!")

    sc.stop()
    println("ID: "+ID+"Query 4's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }

}
