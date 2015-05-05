import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT10  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

  val conf = new SparkConf()

  conf.setAppName("TPCH-Q10")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q10")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 10 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""select c_custkey,c_name,sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment
                                from customer,orders,lineitem,nation
                                where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_returnflag = 'R' and c_nationkey = n_nationkey
                                group by c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment
                                order by revenue desc
                                limit 20""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 10 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 10's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 10 completed!")

    sc.stop()
    println("ID: "+ID+"Query 10's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }

}
