import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT20  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q20")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q18")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 20 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""drop view q20_tmp1_cached""")

    val res1 = sqlContext.sql("""drop view q20_tmp2_cached""")

    val res2 = sqlContext.sql("""drop view q20_tmp3_cached""")

    val res3 = sqlContext.sql("""drop view q20_tmp4_cached""")

    val res4 = sqlContext.sql("""create view q20_tmp1_cached as
                                  select distinct p_partkey
                                from part
                                  where p_name like 'forest%'""")

    val res5 = sqlContext.sql("""create view q20_tmp2_cached as
                                  select l_partkey,l_suppkey,0.5 * sum(l_quantity) as sum_quantity
                                from lineitem
                                  where l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01'
                                group by l_partkey, l_suppkey""")

    val res6 = sqlContext.sql("""create view q20_tmp3_cached as
                                  select ps_suppkey, ps_availqty, sum_quantity
                                from partsupp, q20_tmp1_cached, q20_tmp2_cached
                                where ps_partkey = p_partkey and ps_partkey = l_partkey and ps_suppkey = l_suppkey""")

    val res7 = sqlContext.sql("""create view q20_tmp4_cached as
                                  select ps_suppkey
                                  from q20_tmp3_cached
                                  where ps_availqty > sum_quantity
                                  group by ps_suppkey""")

    val res8 = sqlContext.sql("""select s_name,s_address
                                from supplier,nation,q20_tmp4_cached
                                where s_nationkey = n_nationkey and n_name = 'CANADA' and s_suppkey = ps_suppkey
                                order by s_name""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 20 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)
      println(res1.queryExecution.executedPlan)
      println(res2.queryExecution.executedPlan)
      println(res3.queryExecution.executedPlan)
      println(res4.queryExecution.executedPlan)
      println(res5.queryExecution.executedPlan)
      println(res6.queryExecution.executedPlan)
      println(res7.queryExecution.executedPlan)
      println(res8.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res8.collect().foreach(println)
      }else{
        res8.count()
      }

      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 21's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 21 completed!")

    sc.stop()
    println("ID: "+ID+"Query 21's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
  }

}














