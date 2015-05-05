import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT7  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

  val conf = new SparkConf()

  conf.setAppName("TPCH-Q7")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q7")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 7 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""select supp_nation,cust_nation,l_year,sum(volume) as revenue
                                from (select n1.n_name as supp_nation,n2.n_name as cust_nation,year(l_shipdate) as l_year,l_extendedprice * (1 - l_discount) as volume
                                from supplier,lineitem,orders,customer,nation n1,nation n2
                                where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ((n1.n_name = 'KENYA' and n2.n_name = 'PERU')or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')) and l_shipdate between '1995-01-01' and '1996-12-31') as shipping
                                group by supp_nation,cust_nation,l_year
                                order by supp_nation,cust_nation,l_year""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 7 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 7's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 7 completed!")

    sc.stop()
    println("ID: "+ID+"Query 7's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }
}
