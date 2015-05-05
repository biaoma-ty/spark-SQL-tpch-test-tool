import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT16  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q16")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q16")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 16 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""drop view supplier_tmp""")

    val res1 = sqlContext.sql("""drop view q16_tmp""")


    val res2 = sqlContext.sql("""create view supplier_tmp as
                                select
                                  s_suppkey
                                from
                                  supplier
                                where
                                  not s_comment like '%Customer%Complaints%'""")
    val res3 = sqlContext.sql("""create view q16_tmp as
                                select
                                  p_brand, p_type, p_size, ps_suppkey
                                from
                                  partsupp ps join part p
                                  on
                                    p.p_partkey = ps.ps_partkey and p.p_brand <> 'Brand#45'
                                    and not p.p_type like 'MEDIUM POLISHED%'
                                  join supplier_tmp s
                                  on
                                    ps.ps_suppkey = s.s_suppkey""")

    val res4 = sqlContext.sql("""select
                                  p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
                                from
                                  (select
                                     *
                                   from
                                     q16_tmp
                                   where p_size = 49 or p_size = 14 or p_size = 23 or
                                         p_size = 45 or p_size = 19 or p_size = 3 or
                                         p_size = 36 or p_size = 9
                                ) q16_all
                                group by p_brand, p_type, p_size
                                order by supplier_cnt desc, p_brand, p_type, p_size""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 16 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

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
      println("ID: "+ID+"query 16's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 16 completed!")

    sc.stop()
    println("ID: "+ID+"Query 16's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
  }
}
