package anp.bussiness.transform

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SegmentTransform extends CommonTransform {

    val segmentSchema = StructType(Array(
        StructField("ANO", IntegerType, nullable = false),
        StructField("SEGMENTO", StringType, nullable = false),
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
            log.info(s"Initial read xls segment... -> ")
            val ws = "segment"
            val position="!A1:P5000"
            val bulkSegment = readWorkSheet(spark, ws, position, segmentSchema)
            bulkSegment.show
        } catch {
            case e: Exception => log.info(e.printStackTrace())
        }
    }

    // def aggRegion(df: DataFrame, region: String="AMAZONAS", month: String="Jan"): DataFrame = {
    //     val region = df.filter(col("ESTADO")===region)
    //         .agg(sum(col(month)))
    //     region.show
    //     region
    // }


}

