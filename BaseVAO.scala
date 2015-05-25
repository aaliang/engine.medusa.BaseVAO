package tellerum.common.medusa

import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.{ OrientDynaElementIterable, OrientGraphFactory }
import scala.collection.Map
import scala.collection.JavaConversions._
import com.typesafe.config.ConfigFactory

/**
 * Base OrientDb Vertex Access Object. Generic patterns can be added to here
 */
trait BaseVAO {

  implicit val factory = BaseVAO.factory

  val clazz: String

  def theClass = s"class:$clazz"

  /**
   * Adds vertices given any {scala.collection.Map} instance
   * @param fmap
   */
  def addVertexTx(fmap: Map[String, Any]) {
    val transaction = factory.getTx

    transaction.addVertex(theClass,
      mapAsJavaMap(fmap.filter(x => x._2.!=(None)))
    )

    transaction.commit
  }

  /**
   * Adds vertices given any case class instance.
   * @param icc
   */
  def addVertexTx(icc: Product) {

    val fmap = icc.getClass.getDeclaredFields
      .map(_.getName)
      .zip(icc.productIterator.to)
      .toMap

    addVertexTx(fmap)

  }

  def execFlatQuery(q: String, qparams: List[String]) = {
    val noTx = factory.getNoTx

    val sx: OrientDynaElementIterable = noTx.command(new OCommandSQL(q))
                                            .execute(qparams: _*)

    asScalaIterator(sx.iterator).toList
  }

  def execFlatQuery(q: String, qparams: Map[String, String]) = {
    val noTx = factory.getNoTx

    val sx: OrientDynaElementIterable = noTx.command(new OCommandSQL(q))
      .execute(mapAsJavaMap(qparams))

    asScalaIterator(sx.iterator).toList
  }
}

object BaseVAO {
  val (conf) = ConfigFactory.load()

  //  e.g.
  //  orientdb {
  //    iUri = "remote:localhost/core"
  //    userName = "root"
  //    password = "admin"
  //  }

  val factory = new OrientGraphFactory(
    conf.getString("orientdb.iUri"),
    conf.getString("orientdb.userName"),
    conf.getString("orientdb.password")).setupPool(1, 10)
}
