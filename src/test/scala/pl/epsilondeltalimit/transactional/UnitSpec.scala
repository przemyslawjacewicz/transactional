package pl.epsilondeltalimit.transactional

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalactic.Equality
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.util.{Failure, Success, Try}

trait UnitSpec extends TestSparkSessionProvider with Matchers with DatasetComparer {
  // ----- test support ----- //
  trait TempDirectory {
    val tempDirectoryPath: String = Files.createTempDirectory(null).toString
  }

  // ----- implicits ----- //
  implicit class StringOps(s: String) {
    def /(other: String): String =
      new Path(s, other).toString
  }

  implicit val dataFrameEq: Equality[DataFrame] = (a: DataFrame, b: Any) =>
    b match {
      case bDS: Dataset[_] =>
        Try(
          assertSmallDatasetEquality(
            a.select(a.columns.min, a.columns.sorted.tail: _*),
            bDS.select(bDS.columns.min, bDS.columns.sorted.tail: _*).toDF(),
            ignoreNullable = true,
            orderedComparison = false
          )) match {
          case Success(_)  => true
          case Failure(ex) => throw ex
        }
      case _ =>
        false
    }

}
