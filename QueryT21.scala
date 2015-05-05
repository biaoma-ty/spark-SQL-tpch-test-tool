import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT21  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q21")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q21")
    println("query 21 will be submitted")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 21 will be parsed")
    val chooseDatabase = sqlContext.sql("use "+ParamSet.database)
    chooseDatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""drop view q21_tmp1_cached""")
    val res1 = sqlContext.sql("""drop view q21_tmp2_cached""")

    val res2 = sqlContext.sql("""create view q21_tmp1_cached as
                                select l_orderkey,count(distinct l_suppkey) as count_suppkey,max(l_suppkey) as max_suppkey
                                from lineitem
                                where l_orderkey is not null
                                group by l_orderkey""")

    val res3 = sqlContext.sql("""create view q21_tmp2_cached as
                                  select l_orderkey,count(distinct l_suppkey) count_suppkey,max(l_suppkey) as max_suppkey
                                from lineitem
                                  where l_receiptdate > l_commitdate and l_orderkey is not null
                                group by l_orderkey""")

    val res4 = sqlContext.sql("""select s_name,count(1) as numwait
                                from (select s_name from (select s_name,t2.l_orderkey,l_suppkey,count_suppkey,max_suppkey from q21_tmp2_cached t2 right outer join (select s_name,l_orderkey,l_suppkey from (select s_name,t1.l_orderkey,l_suppkey,count_suppkey,max_suppkey from q21_tmp1_cached t1 join (select s_name,l_orderkey,l_suppkey from orders o join (select s_name,l_orderkey,l_suppkey from nation n join supplier s on s.s_nationkey = n.n_nationkey and n.n_name = 'SAUDI ARABIA' join lineitem l on s.s_suppkey = l.l_suppkey where l.l_receiptdate > l.l_commitdate and l.l_orderkey is not null) l1 on o.o_orderkey = l1.l_orderkey and o.o_orderstatus = 'F') l2 on l2.l_orderkey = t1.l_orderkey) a where (count_suppkey > 1) or ((count_suppkey=1) and (l_suppkey <> max_suppkey))) l3 on l3.l_orderkey = t2.l_orderkey) b where (count_suppkey is null) or ((count_suppkey=1) and (l_suppkey = max_suppkey))) c group by s_name order by numwait desc,s_name
                                limit 100""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 21 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

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
      println("ID: "+ID+"query 21's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 21 completed!")

    sc.stop()
    println("ID: "+ID+"Query 21's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
  }

}



