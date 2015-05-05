import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT8  extends BaseQuery{

  System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

  val conf = new SparkConf()

  conf.setAppName("TPCH-Q8")

  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  override def execute: Unit ={

//    setAppName("TPC-H_Q9")

    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 8 will be parsed")
    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
//    val res0 = sqlContext.sql("""select o_year,sum(case when nation = 'PERU' then volume else 0 end) / sum(volume) as mkt_share
//                                from (select year(o_orderdate) as o_year,l_extendedprice * (1 - l_discount) as volume,n2.n_name as nation
//                                from part,supplier,lineitem,orders,customer,nation n1,nation n2,region
//                                where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'ECONOMY BURNISHED NICKEL' ) as all_nations
//                                group by o_year
//                                order by o_year""")
    val res0 = sqlContext.sql("""insert overwrite table q8_national_market_share select o_year, sum(case when nation = 'BRAZIL' then volume else 0.0 end) / sum(volume) as mkt_share from ( select year(o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from nation n2 join (select o_orderdate, l_discount, l_extendedprice, s_nationkey from supplier s join (select o_orderdate, l_discount, l_extendedprice, l_suppkey from part p join (select o_orderdate, l_partkey, l_discount, l_extendedprice, l_suppkey from lineitem l join (select o_orderdate, o_orderkey from orders o join (select c.c_custkey from customer c join (select n1.n_nationkey from nation n1 join region r on n1.n_regionkey = r.r_regionkey and r.r_name = 'AMERICA' ) n11 on c.c_nationkey = n11.n_nationkey ) c1 on c1.c_custkey = o.o_custkey ) o1 on l.l_orderkey = o1.o_orderkey and o1.o_orderdate >= '1995-01-01' and o1.o_orderdate < '1996-12-31' ) l1 on p.p_partkey = l1.l_partkey and p.p_type = 'ECONOMY ANODIZED STEEL' ) p1 on s.s_suppkey = p1.l_suppkey ) s1 on s1.s_nationkey = n2.n_nationkey ) all_nation group by o_year order by o_year""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 8 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")

    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)

    }else{
      if (ParamSet.showResult){
        res0.collect().foreach(println)
      }else{
        res0.count()
      }
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 8's execution time : " + (t1 - t0) / 1000000000.0 + " secs")
    }
    println("ID: "+ID+"Query 8 completed!")

    sc.stop()
    println("ID: "+ID+"Query 8's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }
}
