import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT9  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))
  val conf = new SparkConf()

  conf.setAppName("TPCH-Q9")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  override def execute: Unit ={

//    setAppName("TPC-H_Q9")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 9 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
//    val res0 = sqlContext.sql("""select nation,o_year,sum(amount) as sum_profit
//                                from (select n_name as nation,year(o_orderdate) as o_year,l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
//                                from part,supplier,lineitem,partsupp,orders,nation
//                                where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%plum%') as profit
//                                group by nation,o_year
//                                order by nation,o_year desc""")

    val res0 = sqlContext.sql(
      """select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, year(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) -  ps_supplycost * l_quantity as amount from orders o join (select l_extendedprice, l_discount, l_quantity, l_orderkey, n_name, ps_supplycost from part p join (select l_extendedprice, l_discount, l_quantity, l_partkey, l_orderkey, n_name, ps_supplycost from partsupp ps join (select l_suppkey, l_extendedprice, l_discount, l_quantity, l_partkey, l_orderkey, n_name from (select s_suppkey, n_name from nation n join supplier s on n.n_nationkey = s.s_nationkey ) s1 join lineitem l on s1.s_suppkey = l.l_suppkey ) l1 on ps.ps_suppkey = l1.l_suppkey and ps.ps_partkey = l1.l_partkey ) l2 on p.p_name like '%green%' and p.p_partkey = l2.l_partkey ) l3 on o.o_orderkey = l3.l_orderkey )profit group by nation, o_year order by nation, o_year desc""")

      t1 = System.nanoTime : Double
    println("ID: "+ID+"query 9 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 9's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 9 completed!")

    sc.stop()
    println("ID: "+ID+"Query 9's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }

}
