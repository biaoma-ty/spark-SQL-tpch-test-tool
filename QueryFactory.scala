
/**
 * Created by ma on 15-1-27.
 */
object QueryFactory {
  def getQuery(num:String):BaseQuery={

//    val query =      Class.forName("QueryT"+num).getDeclaredConstructor(Class.forName("java.lang.String")).newInstance("TPC-h:Q"+num).asInstanceOf[BaseQuery]
    val query = Class.forName("QueryT"+num).newInstance().asInstanceOf[BaseQuery]
    query
  }

//  def getSQuery(num:String):Method={
////    val method =
//  }
}
