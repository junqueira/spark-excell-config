package anp.bussiness.transform

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object FuelTransform extends CommonTransform {

    val fuelSchema = StructType(Array(
<<<<<<< HEAD
        StructField("COMBUSTÍVEL", StringType, nullable = true),
        StructField("ANO", IntegerType, nullable = true),
        StructField("REGIÃO", StringType, nullable = true),
        StructField("ESTADO", StringType, nullable = true),
        StructField("Jan", DoubleType, nullable = true),
        StructField("Fev", DoubleType, nullable = true),
        StructField("Mar", DoubleType, nullable = true),
        StructField("Abr", DoubleType, nullable = true),
        StructField("Maio", DoubleType, nullable = true),
        StructField("Jun", DoubleType, nullable = true),
        StructField("Jul", DoubleType, nullable = true),
        StructField("Ago", DoubleType, nullable = true),
        StructField("Set", DoubleType, nullable = true),
        StructField("Out", DoubleType, nullable = true),
        StructField("Nov", DoubleType, nullable = true),
        StructField("Dez", DoubleType, nullable = true),
        StructField("Total", DoubleType, nullable = true)))

    def execute(spark: SparkSession): DataFrame = {
        log.info(s"Initial read xls fuel... -> ")
        val worksheet = "fuel"
        val sheetPos = "!A1:Q5000"
        val ws = mergeWorkSheets(spark, worksheet, sheetPos, fuelSchema)
        println(s"worksheet: $worksheet quant-> " + ws.count)
        aggRegion(ws, "SÃO PAULO", "2017").show
        ws
=======
        StructField("COMBUSTÍVEL", StringType, nullable = false),
        StructField("ANO", IntegerType, nullable = false)))

    val fuelSchema = StructType(Array(
        StructField("COMBUSTÍVEL", StringType, nullable = false),
        StructField("ANO", IntegerType, nullable = false),
        StructField("REGIÃO", StringType, nullable = false),
        StructField("ESTADO", StringType, nullable = false),
        StructField("Jan", DoubleType, nullable = false),
        StructField("Fev", DoubleType, nullable = false),
        StructField("Mar", DoubleType, nullable = false),
        StructField("Abr", DoubleType, nullable = false),
        StructField("Maio", DoubleType, nullable = false),
        StructField("Jun", DoubleType, nullable = false),
        StructField("Jul", DoubleType, nullable = false),
        StructField("Ago", DoubleType, nullable = false),
        StructField("Set", DoubleType, nullable = false),
        StructField("Out", DoubleType, nullable = false),
        StructField("Nov", DoubleType, nullable = false),
        StructField("Dez", DoubleType, nullable = false),
        StructField("Total", DoubleType, nullable = false)))

    def execute(spark: SparkSession): Unit = {
        try {
            log.info(s"Initial read xls fuel... -> ")
            val worksheet = "fuel"
            val sheetPos = "!A1:Q5000"
            mergeWorkSheets(spark, worksheet, sheetPos, fuelSchema)
        } catch {
            case e: Exception => log.info(e.printStackTrace())
        }
>>>>>>> add commit
    }

  


}

