package pl.epsilondeltalimit.transactional

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import pl.epsilondeltalimit.transactional.SingleWithDeltaVersionBackup._
import pl.epsilondeltalimit.transactional.ThrowWhenNot.throwWhenNotTrue
import pl.epsilondeltalimit.transactional.Using.implicits._
import pl.epsilondeltalimit.transactional.Using.usingFileSystem

case class SingleWithDeltaVersionBackup[O](override val source: String, override val output: String => O)(
    implicit spark: SparkSession)
    extends Single[String, O, Option[Long]] {
  override val backup: String => Option[Long]          = getVersion
  override val restore: Option[Long] => String => Unit = b => s => restoreToVersion(s, b)
  override val cleanup: Option[Long] => Unit           = _ => ()
}

object SingleWithDeltaVersionBackup {
  def getVersion(path: String)(implicit spark: SparkSession): Option[Long] =
    if (DeltaTable.isDeltaTable(path)) {
      import spark.implicits._
      Some(DeltaTable.forPath(path).history().select(max("version")).as[Long].first())
    } else {
      None
    }

  def restoreToVersion(path: String, version: Option[Long])(implicit spark: SparkSession): Unit =
    version match {
      case Some(v) =>
        DeltaTable.forPath(path).restoreToVersion(v)
      case None =>
        throwWhenNotTrue(usingFileSystem(_.delete(new Path(path), true)))
    }

}
