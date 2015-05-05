import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT5  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

  val conf = new SparkConf()

  conf.setAppName("TPCH-Q5")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q5")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 5 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue
                                from customer,orders,lineitem,supplier,nation,region
                                where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' and o_orderdate >= '1993-01-01' and o_orderdate < '1994-01-01'
                                group by n_name
                                order by revenue desc""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 5 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{

      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }

      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 5's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 5 completed!")

    sc.stop()
    println("ID: "+ID+"Query 5's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }


}