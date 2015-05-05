import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT11  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q11")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q11")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 11 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
//    val res0 = sqlContext.sql("""drop view q11_part_tmp_cached""")
//    val res1 = sqlContext.sql("""drop view q11_sum_tmp_cached""")
//    val res2 = sqlContext.sql("""create view q11_part_tmp_cached as
//                                select ps_partkey,sum(ps_supplycost * ps_availqty) as part_value
//                                from partsupp,supplier,nation
//                                where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
//                                group by ps_partkey""")
//
//    val res3 = sqlContext.sql("""create view q11_sum_tmp_cached as
//                                select sum(part_value) as total_value
//                                from q11_part_tmp_cached""")
//
//    val res4 = sqlContext.sql("""select ps_partkey, part_value as value
//                                from (select	ps_partkey,part_value,total_value
//                                from q11_part_tmp_cached join q11_sum_tmp_cached) a
//                                where part_value > total_value * 0.0001
//                                order by value desc""")

    val res0 = sqlContext.sql("""insert overwrite table q11_part_tmp select ps_partkey, sum(ps_supplycost * ps_availqty) as part_value from nation n join supplier s on s.s_nationkey = n.n_nationkey and n.n_name = 'GERMANY' join partsupp ps on ps.ps_suppkey = s.s_suppkey group by ps_partkey""")

    val res1 = sqlContext.sql("""insert overwrite table q11_sum_tmp select sum(part_value) as total_value from q11_part_tmp""")

    val res2 = sqlContext.sql("""select ps_partkey, part_value as value from ( select ps_partkey, part_value, total_value from q11_part_tmp join q11_sum_tmp ) a where part_value > total_value * 0.0001 order by value desc""")


    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 11 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)
      println(res1.queryExecution.executedPlan)
      println(res2.queryExecution.executedPlan)
//      println(res3.queryExecution.executedPlan)
//      println(res4.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res2.collect().foreach(println)
      }else{
        res2.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 11's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 11 completed!")

    sc.stop()
    println("ID: "+ID+"Query 11's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
    "OK"
  }

}
