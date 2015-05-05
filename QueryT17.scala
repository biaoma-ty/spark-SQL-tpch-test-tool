import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT17  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q17")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q17")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 17 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""drop view q17_lineitem_tmp_cached""")
    val res1 = sqlContext.sql("""create view q17_lineitem_tmp_cached as
                                  select l_partkey as t_partkey,0.2 * avg(l_quantity) as t_avg_quantity
                                from lineitem
                                  group by l_partkey""")

    val res2 = sqlContext.sql("""select sum(l_extendedprice) / 7.0 as avg_yearly
                                from (select l_quantity,l_extendedprice,t_avg_quantity from q17_lineitem_tmp_cached join (select l_quantity,l_partkey,l_extendedprice from part,lineitem where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX') l1 on l1.l_partkey = t_partkey) a
                                where l_quantity < t_avg_quantity""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 17 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

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
      println("ID: "+ID+"query 17's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 17 completed!")

    sc.stop()
    println("ID: "+ID+"Query 17's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
  }

}
