package pl.epsilondeltalimit

import org.apache.spark.sql.SparkSession

trait TestSparkSessionProvider {
  implicit lazy val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
    SparkSession.builder
      .appName("TestSparkSession")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  )
}
