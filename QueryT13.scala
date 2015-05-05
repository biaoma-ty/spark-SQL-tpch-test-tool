import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT13  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q13")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  override def execute: Unit ={

//    setAppName("TPC-H_Q13")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 13 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""select c_count,count(*) as custdist
                                from (select c_custkey,count(o_orderkey) as c_count
                                from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%'
                                group by c_custkey) c_orders
                                group by c_count
                                order by custdist desc,c_count desc""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 13 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 13's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 13 completed!")

    sc.stop()
    println("ID: "+ID+"Query 13's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
    "OK"
  }

}
