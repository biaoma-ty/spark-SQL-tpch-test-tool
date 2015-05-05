import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT19  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q19")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={
//    setAppName("TPC-H_Q19")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 19 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
    val res0 = sqlContext.sql("""select sum(l_extendedprice * (1 - l_discount) ) as revenue
                                from lineitem l join part p
                                  on p.p_partkey = l.l_partkey
                                where (p_brand = 'Brand#12' and p_container REGEXP 'SM CASE||SM BOX||SM PACK||SM PKG' and l_quantity >= 1 and l_quantity <= 11 and p_size >= 1 and p_size <= 5 and l_shipmode REGEXP 'AIR||AIR REG' and l_shipinstruct = 'DELIVER IN PERSON') or (p_brand = 'Brand#23' and p_container REGEXP 'MED BAG||MED BOX||MED PKG||MED PACK' and l_quantity >= 10 and l_quantity <= 20 and p_size >= 1 and p_size <= 10 and l_shipmode REGEXP 'AIR||AIR REG' and l_shipinstruct = 'DELIVER IN PERSON') or (p_brand = 'Brand#34' and p_container REGEXP 'LG CASE||LG BOX||LG PACK||LG PKG' and l_quantity >= 20 and l_quantity <= 30 and p_size >= 1 and p_size <= 15 and l_shipmode REGEXP 'AIR||AIR REG' and l_shipinstruct = 'DELIVER IN PERSON')""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 19 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 19's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 19 completed!")

    sc.stop()
    println("ID: "+ID+"Query 19's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)

  }
}