import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT18  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q18")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q18")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 18 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""drop view q18_tmp_cached""")
    val res1 = sqlContext.sql("""create view q18_tmp_cached as
                                select l_orderkey,sum(l_quantity) as t_sum_quantity
                                from lineitem
                                where l_orderkey is not null
                                group by l_orderkey""")

    val res2 = sqlContext.sql("""select c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum(l_quantity)
                                from customer,orders,q18_tmp_cached t,lineitem l
                                where c_custkey = o_custkey and o_orderkey = t.l_orderkey and o_orderkey is not null and t.t_sum_quantity > 300 and o_orderkey = l.l_orderkey and l.l_orderkey is not null
                                group by c_name, c_custkey,o_orderkey,o_orderdate,o_totalprice
                                order by o_totalprice desc, o_orderdate
                                limit 100""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 18 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)
      println(res1.queryExecution.executedPlan)
      println(res2.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res2.collect().foreach(println)
      }else{
        res2.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 18's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 18 completed!")

    sc.stop()
    println("ID: "+ID+"Query 18's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
  }

}
