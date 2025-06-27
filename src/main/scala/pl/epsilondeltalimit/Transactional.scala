package pl.epsilondeltalimit

import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import pl.epsilondeltalimit.ThrowWhenNot.throwWhenNotTrue
import pl.epsilondeltalimit.Using.implicits.javaIoClosable
import pl.epsilondeltalimit.Using.usingFileSystem

import scala.util.{Failure, Success, Try}

object Transactional extends LazyLogging {

  case class Single[S, O, B](source: S, output: S => O, backup: S => B, restore: B => S => Unit, cleanup: B => Unit)

  def transactional[S, O, B](s: Single[S, O, B], ss: Single[S, O, B]*): Try[Seq[O]] = {
    val sss = s +: ss
    logger.info(s"[TX:START] Starting operations on sources: ${sss.map(_.source).mkString(",")}.")

    val (updated, result) =
      sss.foldLeft((FirstWriteWinsMap.empty[S, (B, B => Unit)], Success(Seq.empty[O]): Try[Seq[O]])) {
        case (acc, Single(source, getOutput, getBackup, getRestore, getCleanup)) =>
          acc match {
            case (_, _: Failure[_]) =>
              acc
            case (updated, Success(os)) =>
              val newUpdated = updated.add(source, (getBackup(source), getCleanup))
              Try(getOutput(source)) match {
                case Failure(ex) =>
                  logger.error(s"[TX] Operation failed for source: $source. Restoring sources.", ex)
                  updated.underlying match {
                    case underlying if underlying.nonEmpty =>
                      logger.warn(s"[TX] Restoring sources: ${updated.underlying.map(_._1).mkString(",")}.")
                      updated.underlying.foreach { case (s, (b, _)) =>
                        Try(getRestore(b)(s)) match {
                          case Failure(ex) =>
                            logger.error(s"[TX] Failed to restore source: source=$s, backup=$b.", ex)
                          case Success(_) =>
                            logger.info(s"[TX] Successfully restored source: source=$s, backup=$b.")
                        }
                      }
                    case _ =>
                      logger.warn(s"[TX] No sources updated, nothing to restore.")
                  }
                  (newUpdated, Failure(ex))
                case Success(o) =>
                  logger.info(s"[TX] Operation succeeded for source: $source.")
                  (newUpdated, Success(o +: os))
              }
          }
      }

    updated.underlying.foreach { case (source, (backup, getCleanup)) =>
      logger.info(s"[TX] Cleaning up backup for source: backup=$backup, source=$source.")
      Try(getCleanup(backup)) match {
        case Failure(ex) =>
          logger.error(s"[TX] Failed to cleanup backup for source: backup=$backup, source=$source.", ex)
        case Success(_) =>
          logger.info(s"[TX] Successfully cleaned up backup for source: backup=$backup, source=$source.")
      }
    }

    result match {
      case _: Failure[_] =>
        logger.info(s"[TX:END_FAILURE] Finished operations on sources: ${sss.map(_.source).mkString(",")}.")
      case _: Success[_] =>
        logger.info(s"[TX:END_SUCCESS] Finished operations on sources: ${sss.map(_.source).mkString(",")}.")
    }
    result
  }

  def delta[O](p: (String, SparkSession => String => O), ps: (String, SparkSession => String => O)*)(
      implicit spark: SparkSession): Try[Seq[O]] = {
    import spark.implicits._

    def getVersion(path: String): Option[Long] =
      if (DeltaTable.isDeltaTable(path)) {
        Some(DeltaTable.forPath(path).history().select(max("version")).as[Long].first())
      } else {
        None
      }

    def restoreToVersion(path: String, version: Option[Long]): Unit =
      version match {
        case Some(v) =>
          DeltaTable.forPath(path).restoreToVersion(v)
        case None =>
          usingFileSystem(_.delete(new Path(path), true))
      }

    def single(in: (String, SparkSession => String => O)): Single[String, O, Option[Long]] =
      Single(
        source = in._1,
        output = s => in._2(spark)(s),
        backup = s => getVersion(s),
        restore = b => s => restoreToVersion(s, b),
        cleanup = _ => ()
      )

    transactional(s = single(p), ss = ps.map(single): _*)
  }

  def parquet[O](p: (String, SparkSession => String => O), ps: (String, SparkSession => String => O)*)(
      implicit spark: SparkSession): Try[Seq[O]] = {

    def rw(from: String, to: String): String = {
      spark.read.format("parquet").load(from).write.format("parquet").mode("overwrite").save(to)
      to
    }

    def delete(p: String): Unit =
      usingFileSystem(_.delete(new Path(p), true))

    def single(in: (String, SparkSession => String => O)): Single[String, O, String] =
      Single(
        source = in._1,
        output = s => in._2(spark)(s),
        backup = s => rw(s, s + ".bak"),
        restore = b => s => rw(b, s),
        cleanup = delete
      )

    transactional(s = single(p), ss = ps.map(single): _*)
  }

  def backup[O](p: (String, SparkSession => String => O), ps: (String, SparkSession => String => O)*)(
    implicit spark: SparkSession): Try[Seq[O]] = {

    def copy(from: String, to: String): String = {
      usingFileSystem { fs =>
        throwWhenNotTrue {
          FileUtil.copy(fs, new Path(from), fs, new Path(to), false, true, spark.sparkContext.hadoopConfiguration)
        }
      }
      to
    }

    def delete(p: String): Unit =
      usingFileSystem(fs => throwWhenNotTrue(fs.delete(new Path(p), true)))

    def single(in: (String, SparkSession => String => O)): Single[String, O, String] =
      Single(
        source = in._1,
        output = s => in._2(spark)(s),
        backup = s => copy(s, s + ".bak"),
        restore = b => s => copy(b, s),
        cleanup = delete
      )

    transactional(s = single(p), ss = ps.map(single): _*)
  }

}
