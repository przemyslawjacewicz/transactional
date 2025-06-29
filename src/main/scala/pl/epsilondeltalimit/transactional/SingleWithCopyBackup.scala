package pl.epsilondeltalimit.transactional

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession
import pl.epsilondeltalimit.transactional.SingleWithCopyBackup.delete
import pl.epsilondeltalimit.transactional.ThrowWhenNot.throwWhenNotTrue
import pl.epsilondeltalimit.transactional.Using.implicits._
import pl.epsilondeltalimit.transactional.Using.usingFileSystem

//todo: consider a better handling of backup location
//todo: consider removing cleanup with default behavior of creating backup in temp location that does not need cleanup
case class SingleWithCopyBackup[O](override val source: String,
                                   override val output: String => O,
                                   copy: SparkSession => (String, String) => Unit)(implicit spark: SparkSession)
    extends Single[String, O, String] {
  override val backup: String => String          = s => { val b = s + ".bak"; copy(spark)(s, b); b; }
  override val restore: String => String => Unit = b => s => copy(spark)(b, s)
  override val cleanup: String => Unit           = delete
}

object SingleWithCopyBackup {
  def delete(p: String)(implicit spark: SparkSession): Unit =
    throwWhenNotTrue(usingFileSystem(_.delete(new Path(p), true)))

  def SparkCopy[O](source: String, output: String => O, format: String)(
      implicit spark: SparkSession): SingleWithCopyBackup[O] =
    SingleWithCopyBackup[O](
      source,
      output,
      spark => (from, to) => spark.read.format(format).load(from).write.format(format).mode("overwrite").save(to)
    )

  def FileSystemCopy[O](source: String, output: String => O)(implicit spark: SparkSession): SingleWithCopyBackup[O] =
    SingleWithCopyBackup[O](
      source,
      output,
      _ =>
        (from, to) =>
          throwWhenNotTrue {
//            implicit val _spark: SparkSession = spark
            usingFileSystem { fs =>
              FileUtil.copy(fs, new Path(from), fs, new Path(to), false, true, spark.sparkContext.hadoopConfiguration)
            }//(spark, implicitly[Using.Closeable[FileSystem]])
          }
    )
}
