package anp.bussiness.transform

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory


trait CommonTransform {
    val log: Logger = Logger.getLogger(this.getClass)
    val configManager = ConfigFactory.load()
    val file = configManager.getString(s"sheet.path_hdfs")

    def mergeWorkSheets(spark: SparkSession, worksheet: String, schema: StructType): Unit = {
        for (i <- 1 to 3) {
            val position = decimalToBinary(i.toInt)
            val ws = configManager.getConfig(s"sheet.$worksheet.worksheets.$position").root.get("ws").render
            val sheetPos = configManager.getConfig(s"sheet.$worksheet.worksheets.$position").root.get("pos").render
            val sheet = readWorkSheet(spark, ws, sheetPos, schema)
            sheet.show
        }
    }

    def readWorkSheet(spark: SparkSession, ws: String, sheetPos: String, schema: StructType): DataFrame = {       
        val dataAddress = s"$ws$sheetPos"
        val file = configManager.getString(s"sheet.path_hdfs")
        // var schema = getSchema(ws)
        readXls(spark, file, dataAddress, schema)
    }

    // def getSchema(ws: ConfigFactory): StructType = {
    //     var schema: Array()
    //     for (i <- 1 to 17) {
    //         val position = decimalToBinary(i)
    //         col = ws.getConfig(position).root.get("collumn")
    //         typ = ws.getConfig(position).root.get("type")
    //         schema.append(s"StructField($col, $typ, nullable = false)")
    //     }
    //     StructType(schema)
    // }

    def readXls(spark: SparkSession, file: String, dataAddress: String, schema: StructType): DataFrame = {
        spark.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("dataAddress", dataAddress)
            .option("inferSchema", true)
            .schema(schema)
            .load(file)
    }

	def decimalToBinary(num: Int, leng: Int=5): String = {
		var flag: Int = 0;
     	var number: Int = num;
        var res =""
		if (number < 0)
		{
			number = -number;
		}
		var bits: Int = 31;
		while (bits >= 0)
		{
			if (((number >> bits) & 1) == 1)
			{
                res += "1"
				flag = 1;
			}
			else if (flag == 1)
			{
                res += "0"
			}
			bits -= 1;
		}
        s"%0${leng}d".format(res.toInt).toString
	}
}
