package pl.epsilondeltalimit

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object Using {

  //  def using[U <: { def close(): Unit }, R](u: U)(r: U => R): R =
  //    try r(u)
  //    finally u.close()

  trait Closeable[T] {
    def close(resource: T): Unit
  }

  def using[R, O](resource: R)(r: R => O)(implicit closeable: Closeable[R]): O =
    try r(resource)
    finally closeable.close(resource)

  def usingFileSystem[R](r: FileSystem => R)(implicit spark: SparkSession, c: Closeable[FileSystem]): R =
    using(FileSystem.newInstance(spark.sparkContext.hadoopConfiguration))(r)

  object implicits {
    implicit def javaIoClosable[T <: java.io.Closeable]: Closeable[T] =
      (resource: T) => resource.close()
  }
}
