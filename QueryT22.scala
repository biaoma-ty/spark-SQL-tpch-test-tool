import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT22  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q22")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q21")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 22 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""drop view q22_customer_tmp_cached""")
    val res1 = sqlContext.sql("""drop view q22_customer_tmp1_cached""")

    val res2 = sqlContext.sql("""drop view q22_orders_tmp_cached""")

    val res3 = sqlContext.sql("""create view if not exists q22_customer_tmp_cached as
                                select c_acctbal,c_custkey,substr(c_phone, 1, 2) as cntrycode
                                from customer
                                where substr(c_phone, 1, 2) = '13' or substr(c_phone, 1, 2) = '31' or substr(c_phone, 1, 2) = '23' or substr(c_phone, 1, 2) = '29' or substr(c_phone, 1, 2) = '30' or substr(c_phone, 1, 2) = '18' or substr(c_phone, 1, 2) = '17'""")

    val res4 = sqlContext.sql("""create view if not exists q22_customer_tmp1_cached as
                                select avg(c_acctbal) as avg_acctbal
                                from q22_customer_tmp_cached
                                where c_acctbal > 0.00""")
    val res5 = sqlContext.sql("""create view if not exists q22_orders_tmp_cached as
                                select o_custkey
                                from orders
                                group by o_custkey""")

    val res6 = sqlContext.sql("""select cntrycode,count(1) as numcust,sum(c_acctbal) as totacctbal
                                from (select cntrycode,c_acctbal,avg_acctbal from q22_customer_tmp1_cached ct1 join (select cntrycode,c_acctbal from q22_orders_tmp_cached ot right outer join q22_customer_tmp_cached ct on ct.c_custkey = ot.o_custkey where o_custkey is null) ct2) a where c_acctbal > avg_acctbal
                                group by cntrycode
                                order by cntrycode""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 22 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")
    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)
      println(res1.queryExecution.executedPlan)
      println(res2.queryExecution.executedPlan)
      println(res3.queryExecution.executedPlan)
      println(res4.queryExecution.executedPlan)
      println(res5.queryExecution.executedPlan)
      println(res6.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res6.collect().foreach(println)
      }else{
        res6.count()
      }

      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 22's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 22 completed!")

    sc.stop()
    println("ID: "+ID+"Query 22's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
  }

}
