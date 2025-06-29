package pl.epsilondeltalimit.transactional

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import pl.epsilondeltalimit.transactional.ThrowWhenNot.throwWhenNotTrue
import pl.epsilondeltalimit.transactional.Transactional._
import pl.epsilondeltalimit.transactional.Using.implicits.javaIoClosable
import pl.epsilondeltalimit.transactional.Using.{using, usingFileSystem}

import java.io.IOException
import scala.io.Source
import scala.util.{Failure, Success, Try}

class TransactionalTest extends UnitFunSpec {
  import spark.implicits._

  describe("transactional with delta table backup") {
    val single: (String, String => Unit) => SingleWithDeltaVersionBackup[Unit] =
      (source, output) => SingleWithDeltaVersionBackup(source, output)

    describe("when executed for the same table") {
      describe("when executed with operations that are successful") {
        it("should update table") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            Seq(0).toDF().write.format("delta").save(path)

            val result: Try[Seq[Unit]] = transactional(
              single(path, Seq(1).toDF().write.format("delta").mode("append").save),
              single(path, Seq(2).toDF().write.format("delta").mode("append").save)
            )

            result should ===(Success(Seq((), ())))
            spark.read.format("delta").load(path) should ===(Seq(0, 1, 2).toDF())
            DeltaTable.forPath(path).history().count() should ===(3)
          }
        }
      }

      describe("when executed with operations that fail") {
        it("should not change table content when first operation fails") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            Seq(0).toDF().write.format("delta").save(path)

            val result: Try[Seq[Unit]] = transactional(
              single(path, _ => throw new RuntimeException("I fail!")),
              single(path, Seq(2).toDF().write.format("delta").mode("append").save)
            )

            result shouldBe a[Failure[_]]
            spark.read.format("delta").load(path) should ===(Seq(0).toDF())
            DeltaTable.forPath(path).history().count() should ===(1)
          }
        }

        it("should not change table content when second operation fails") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            Seq(0).toDF().write.format("delta").save(path)

            val result: Try[Seq[Unit]] = transactional(
              single(path, Seq(1).toDF().write.format("delta").mode("append").save),
              single(path, _ => throw new RuntimeException("I fail!"))
            )

            result shouldBe a[Failure[_]]
            spark.read.format("delta").load(path) should ===(Seq(0).toDF())
            DeltaTable.forPath(path).history().count() should ===(3)
          }
        }
      }
    }

    describe("when executed against different tables") {
      describe("when executed with operations that are successful") {
        it("should update all tables") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            Seq(0).toDF().write.format("delta").save(path1)
            Seq(0).toDF().write.format("delta").save(path2)

            val result: Try[Seq[Unit]] = transactional(
              single(path1, Seq(1).toDF().write.format("delta").mode("append").save),
              single(path2, Seq(1).toDF().write.format("delta").mode("append").save)
            )

            result should ===(Success(Seq((), ())))
            spark.read.format("delta").load(path1) should ===(Seq(0, 1).toDF())
            DeltaTable.forPath(path1).history().count() should ===(2)
            spark.read.format("delta").load(path2) should ===(Seq(0, 1).toDF())
            DeltaTable.forPath(path2).history().count() should ===(2)
          }
        }
      }

      describe("when executed with operations that fail") {
        it("should not change tables content when first operation fails") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            Seq(0).toDF().write.format("delta").save(path1)
            Seq(0).toDF().write.format("delta").save(path2)

            val result: Try[Seq[Unit]] = transactional(
              single(path1, _ => throw new RuntimeException("I fail!")),
              single(path2, Seq(1).toDF().write.format("delta").mode("append").save)
            )

            result shouldBe a[Failure[_]]
            spark.read.format("delta").load(path1) should ===(Seq(0).toDF())
            DeltaTable.forPath(path1).history().count() should ===(1)
            spark.read.format("delta").load(path2) should ===(Seq(0).toDF())
            DeltaTable.forPath(path2).history().count() should ===(1)
          }
        }

        it("should not change tables content when second operation fails") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            Seq(0).toDF().write.format("delta").save(path1)
            Seq(0).toDF().write.format("delta").save(path2)

            val result: Try[Seq[Unit]] = transactional(
              single(path1, Seq(1).toDF().write.format("delta").mode("append").save),
              single(path2, _ => throw new RuntimeException("I fail!"))
            )

            result shouldBe a[Failure[_]]
            spark.read.format("delta").load(path1) should ===(Seq(0).toDF())
            DeltaTable.forPath(path1).history().count() should ===(3)
            spark.read.format("delta").load(path2) should ===(Seq(0).toDF())
            DeltaTable.forPath(path2).history().count() should ===(1)
          }
        }
      }
    }
  }

  describe("transactional with parquet copy backup") {
    val single: (String, String => Unit) => SingleWithCopyBackup[Unit] =
      (source, output) => SingleWithCopyBackup.SparkCopy(source, output, "parquet")

    describe("when executed for the same table") {
      describe("when executed with operations that are successful") {
        it("should update table") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            Seq(0).toDF().write.format("parquet").save(path)

            val result: Try[Seq[Unit]] = transactional(
              single(path, Seq(1).toDF().write.format("parquet").mode("append").save),
              single(path, Seq(2).toDF().write.format("parquet").mode("append").save)
            )

            result should ===(Success(Seq((), ())))
            spark.read.format("parquet").load(path) should ===(Seq(0, 1, 2).toDF())
            usingFileSystem(_.exists(new Path(path + ".bak")) should ===(false))
          }
        }
      }

      describe("when executed with operations that fail") {
        it("should not change table content when first operation fails") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            Seq(0).toDF().write.format("parquet").save(path)

            val result: Try[Seq[Unit]] = transactional(
              single(path, _ => throw new RuntimeException("I fail!")),
              single(path, Seq(2).toDF().write.format("parquet").mode("append").save)
            )

            result shouldBe a[Failure[_]]
            spark.read.format("parquet").load(path) should ===(Seq(0).toDF())
            usingFileSystem(_.exists(new Path(path + ".bak")) should ===(false))
          }
        }

        it("should not change table content when second operation fails") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            Seq(0).toDF().write.format("parquet").save(path)

            val result: Try[Seq[Unit]] = transactional(
              single(path, Seq(1).toDF().write.format("parquet").mode("append").save),
              single(path, _ => throw new RuntimeException("I fail!"))
            )

            result shouldBe a[Failure[_]]
            spark.read.format("parquet").load(path) should ===(Seq(0).toDF())
            usingFileSystem(_.exists(new Path(path + ".bak")) should ===(false))
          }
        }
      }
    }

    describe("when executed against different tables") {
      describe("when executed with operations that are successful") {
        it("should update all tables") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            Seq(0).toDF().write.format("parquet").save(path1)
            Seq(0).toDF().write.format("parquet").save(path2)

            val result: Try[Seq[Unit]] = transactional(
              single(path1, Seq(1).toDF().write.format("parquet").mode("append").save),
              single(path2, Seq(1).toDF().write.format("parquet").mode("append").save)
            )

            result should ===(Success(Seq((), ())))
            spark.read.format("parquet").load(path1) should ===(Seq(0, 1).toDF())
            usingFileSystem(_.exists(new Path(path1 + ".bak")) should ===(false))
            spark.read.format("parquet").load(path2) should ===(Seq(0, 1).toDF())
            usingFileSystem(_.exists(new Path(path2 + ".bak")) should ===(false))
          }
        }
      }

      describe("when executed with operations that fail") {
        it("should not change tables content when first operation fails") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            Seq(0).toDF().write.format("parquet").save(path1)
            Seq(0).toDF().write.format("parquet").save(path2)

            val result: Try[Seq[Unit]] = transactional(
              single(path1, _ => throw new RuntimeException("I fail!")),
              single(path2, Seq(1).toDF().write.format("parquet").mode("append").save)
            )

            result shouldBe a[Failure[_]]
            spark.read.format("parquet").load(path1) should ===(Seq(0).toDF())
            usingFileSystem(_.exists(new Path(path1 + ".bak")) should ===(false))
            spark.read.format("parquet").load(path2) should ===(Seq(0).toDF())
            usingFileSystem(_.exists(new Path(path2 + ".bak")) should ===(false))
          }
        }

        it("should not change tables content when second operation fails") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            Seq(0).toDF().write.format("parquet").save(path1)
            Seq(0).toDF().write.format("parquet").save(path2)

            val result: Try[Seq[Unit]] = transactional(
              single(path1, Seq(1).toDF().write.format("parquet").mode("append").save),
              single(path2, _ => throw new RuntimeException("I fail!"))
            )

            result shouldBe a[Failure[_]]
            spark.read.format("parquet").load(path1) should ===(Seq(0).toDF())
            usingFileSystem(_.exists(new Path(path1 + ".bak")) should ===(false))
            spark.read.format("parquet").load(path2) should ===(Seq(0).toDF())
            usingFileSystem(_.exists(new Path(path2 + ".bak")) should ===(false))
          }
        }
      }
    }
  }

  describe("transactional with copy backup") {
    val single: (String, String => Unit) => SingleWithCopyBackup[Unit] =
      (source, output) => SingleWithCopyBackup.FileSystemCopy(source, output)

    def createFile(path: String, content: String)(implicit spark: SparkSession): Boolean =
      usingFileSystem { fs =>
        try using(fs.create(new Path(path))) { o =>
          o.writeBytes(content)
          o.flush()
          true
        } catch {
          case _: IOException => false
        }
      }

    def appendToFile(path: String, line: String)(implicit spark: SparkSession): Boolean =
      usingFileSystem { fs =>
        if (!fs.exists(new Path(path))) {
          createFile(path, line)
        } else {
          try {
            val lines = getLines(path)
            fs.delete(new Path(path), true) && createFile(path, (lines :+ line).mkString("\n"))
          } catch {
            case _: IOException => false
          }
        }
      }

    def getLines(path: String)(implicit spark: SparkSession): Seq[String] =
      usingFileSystem { fs =>
        if (!fs.exists(new Path(path))) {
          Seq.empty
        } else {
          using(fs.open(new Path(path))) { s =>
            Source.fromInputStream(s).getLines().toList
          }
        }
      }

    describe("when executed for the same table") {
      describe("when executed with operations that are successful") {
        it("should update table") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            throwWhenNotTrue(createFile(path, "0"))

            val result: Try[Seq[Unit]] = transactional(
              single(path, path => throwWhenNotTrue(appendToFile(path, "1"))),
              single(path, path => throwWhenNotTrue(appendToFile(path, "2")))
            )

            result should ===(Success(Seq((), ())))
            getLines(path) should ===(Seq("0", "1", "2"))
            usingFileSystem(_.exists(new Path(path + ".bak")) should ===(false))
          }
        }
      }

      describe("when executed with operations that fail") {
        it("should not change table content when first operation fails") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            throwWhenNotTrue(createFile(path, "0"))

            val result: Try[Seq[Unit]] = transactional(
              single(path, _ => throw new RuntimeException("I fail!")),
              single(path, path => throwWhenNotTrue(appendToFile(path, "2")))
            )

            result shouldBe a[Failure[_]]
            getLines(path) should ===(Seq("0"))
            usingFileSystem(_.exists(new Path(path + ".bak")) should ===(false))
          }
        }

        it("should not change table content when second operation fails") {
          new TempDirectory {
            val path: String = tempDirectoryPath / "table"

            throwWhenNotTrue(createFile(path, "0"))

            val result: Try[Seq[Unit]] = transactional(
              single(path, path => throwWhenNotTrue(appendToFile(path, "1"))),
              single(path, _ => throw new RuntimeException("I fail!"))
            )

            result shouldBe a[Failure[_]]
            getLines(path) should ===(Seq("0"))
            usingFileSystem(_.exists(new Path(path + ".bak")) should ===(false))
          }
        }
      }
    }

    describe("when executed against different tables") {
      describe("when executed with operations that are successful") {
        it("should update all tables") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            throwWhenNotTrue(createFile(path1, "0"))
            throwWhenNotTrue(createFile(path2, "0"))

            val result: Try[Seq[Unit]] = transactional(
              single(path1, path => throwWhenNotTrue(appendToFile(path, "1"))),
              single(path2, path => throwWhenNotTrue(appendToFile(path, "1")))
            )

            result should ===(Success(Seq((), ())))
            getLines(path1) should ===(Seq("0", "1"))
            usingFileSystem(_.exists(new Path(path1 + ".bak")) should ===(false))
            getLines(path2) should ===(Seq("0", "1"))
            usingFileSystem(_.exists(new Path(path2 + ".bak")) should ===(false))
          }
        }
      }

      describe("when executed with operations that fail") {
        it("should not change tables content when first operation fails") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            throwWhenNotTrue(createFile(path1, "0"))
            throwWhenNotTrue(createFile(path2, "0"))

            val result: Try[Seq[Unit]] = transactional(
              single(path1, _ => throw new RuntimeException("I fail!")),
              single(path2, path => throwWhenNotTrue(appendToFile(path, "1")))
            )

            result shouldBe a[Failure[_]]
            getLines(path1) should ===(Seq("0"))
            usingFileSystem(_.exists(new Path(path1 + ".bak")) should ===(false))
            getLines(path2) should ===(Seq("0"))
            usingFileSystem(_.exists(new Path(path2 + ".bak")) should ===(false))
          }
        }

        it("should not change tables content when second operation fails") {
          new TempDirectory {
            val path1: String = tempDirectoryPath / "table1"
            val path2: String = tempDirectoryPath / "table2"

            throwWhenNotTrue(createFile(path1, "0"))
            throwWhenNotTrue(createFile(path2, "0"))

            val result: Try[Seq[Unit]] = transactional(
              single(path1, path => throwWhenNotTrue(appendToFile(path, "1"))),
              single(path2, _ => throw new RuntimeException("I fail!"))
            )

            result shouldBe a[Failure[_]]
            getLines(path1) should ===(Seq("0"))
            usingFileSystem(_.exists(new Path(path1 + ".bak")) should ===(false))
            getLines(path2) should ===(Seq("0"))
            usingFileSystem(_.exists(new Path(path2 + ".bak")) should ===(false))
          }
        }
      }
    }
  }
}
