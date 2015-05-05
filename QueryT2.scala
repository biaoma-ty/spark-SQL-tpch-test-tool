import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ma on 15-1-27.
 */
class QueryT2  extends BaseQuery{



  override def execute: Unit ={
    System.setProperty("spark.cores.max",String.valueOf(ParamSet.cores))

    val conf = new SparkConf()

    conf.setAppName("TPCH-Q2")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)


    //get the time before the query be executed
    val t0 = System.nanoTime : Double
    var t1 = System.nanoTime : Double

    println("ID: "+ID+"query 2 will be parsed")

    val choosDdatabase = sqlContext.sql("use "+ParamSet.database)
    choosDdatabase.count()

    println("DATABASE: "+ParamSet.database)

    //the query
//    val res0 = sqlContext.sql("""drop view q2_min_ps_supplycost""")
//
//    val res1 = sqlContext.sql("""create view q2_min_ps_supplycost as
//                                select p_partkey as min_p_partkey,min(ps_supplycost) as min_ps_supplycost
//                                from part,partsupp,supplier,nation,region
//                                where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE'
//                                group by p_partkey""")
//    val res2 = sqlContext.sql("""select s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment
//                                from part,supplier,partsupp,nation,region,q2_min_ps_supplycost
//                                where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 37 and p_type like '%COPPER' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = min_ps_supplycost and p_partkey = min_p_partkey
//                                order by s_acctbal desc,n_name,s_name,p_partkey
//                                limit 100""")

//    sqlContext.sql("""drop view q2_tmp1""").count()
//
//    sqlContext.sql("""drop view q2_tmp2""").count()
//
//    val res0 = sqlContext.sql("""create view q2_tmp1 as select s.s_acctbal, s.s_name, n.n_name, p.p_partkey, ps.ps_supplycost, p.p_mfgr, s.s_address, s.s_phone, s.s_comment from nation n join region r on n.n_regionkey = r.r_regionkey and r.r_name = 'EUROPE' join supplier s on s.s_nationkey = n.n_nationkey join partsupp ps on s.s_suppkey = ps.ps_suppkey join part p on p.p_partkey = ps.ps_partkey and p.p_size = 15 and p.p_type like '%BRASS'""")
//
//    val res1 = sqlContext.sql("""create view q2_tmp2 as select s.s_acctbal, s.s_name, n.n_name, p.p_partkey, ps.ps_supplycost, p.p_mfgr, s.s_address, s.s_phone, s.s_comment from nation n join region r on n.n_regionkey = r.r_regionkey and r.r_name = 'EUROPE' join supplier s on s.s_nationkey = n.n_nationkey join partsupp ps on s.s_suppkey = ps.ps_suppkey join part p on p.p_partkey = ps.ps_partkey and p.p_size = 15 and p.p_type like '%BRASS'""")
//
//    val res2 = sqlContext.sql("""select t1.s_acctbal, t1.s_name, t1.n_name, t1.p_partkey, t1.p_mfgr, t1.s_address, t1.s_phone, t1.s_comment from q2_tmp1 t1 join q2_tmp2 t2 on t1.p_partkey = t2.p_partkey and t1.ps_supplycost=t2.ps_min_supplycost order by s_acctbal desc, n_name, s_name, p_partkey limit 100""")

    val res0 = sqlContext.sql("""insert overwrite table q2_minimum_cost_supplier_tmp1 select s.s_acctbal, s.s_name, n.n_name, p.p_partkey, ps.ps_supplycost, p.p_mfgr, s.s_address, s.s_phone, s.s_comment from nation n join region r on n.n_regionkey = r.r_regionkey and r.r_name = 'EUROPE' join supplier s on s.s_nationkey = n.n_nationkey join partsupp ps on s.s_suppkey = ps.ps_suppkey join part p on p.p_partkey = ps.ps_partkey and p.p_size = 15 and p.p_type like '%BRASS' """)

    val res1 = sqlContext.sql("""insert overwrite table q2_minimum_cost_supplier_tmp2 select p_partkey, min(ps_supplycost) from q2_minimum_cost_supplier_tmp1 group by p_partkey""")

    val res2 = sqlContext.sql("""select t1.s_acctbal, t1.s_name, t1.n_name, t1.p_partkey, t1.p_mfgr, t1.s_address, t1.s_phone, t1.s_comment from q2_minimum_cost_supplier_tmp1 t1 join q2_minimum_cost_supplier_tmp2 t2 on t1.p_partkey = t2.p_partkey and t1.ps_supplycost=t2.ps_min_supplycost order by s_acctbal desc, n_name, s_name, p_partkey limit 100""")

    t1 = System.nanoTime : Double
    println("ID: "+ID+"query 2 parse done, parse time:"+ (t1 - t0) / 1000000000.0 + " secs")
    if(ParamSet.isExplain){
      println(res0.queryExecution.executedPlan)
      println(res1.queryExecution.executedPlan)
      println(res2.queryExecution.executedPlan)
    }else{

      if (ParamSet.showResult){
        res2.collect().foreach(println)
      }else{
        res0.count()
        res1.count()
        res2.count()
      }

//
      t1 = System.nanoTime : Double
      println("ID: "+ID+"query 2's execution time : " + (t1 - t0) / 1000000000.0 + " secs")

    }
    println("ID: "+ID+"Query 2 completed!")

    sc.stop()
    println("ID: "+ID+"Query 2's context successfully stopped")
    Runtime.getRuntime.exec(ParamSet.execFREE)
//    "OK"
  }

}
