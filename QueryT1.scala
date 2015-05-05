import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT1 extends BaseQuery{


  override def execute: Unit ={

//    setAppName("TPC-H_Q1")
    System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

    val conf = new SparkConf()

    conf.setAppName("TPCH-Q1")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 1 will be parsed")

    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate < '1993-06-27'
                                group by l_returnflag, l_linestatus
                                order by l_returnflag, l_linestatus""")

    t1 = System.nanoTime : Double

    println("ID: "+ID+"query 1 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")
    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)
    }else{

      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }


      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 1's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 1 completed!")

    sc.stop()
    println("ID: "+ID+"Query 1's context successfully stopped")

    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }
}
