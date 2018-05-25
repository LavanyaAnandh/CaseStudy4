//Hospital CaseStudy4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CaseStudy_Hospital {

 def main(args: Array[String]): Unit = {

   //Creating spark Session object
   val spark = SparkSession
     .builder()
     .master("local")
     .appName("Spark SQL basic example")
     .config("spark.some.config.option", "some-value")
     .getOrCreate()

   println("Spark Session Object created")

   //creating schema for the table that need to be loaded from file
  val Manual_schema = new StructType(Array( new StructField("DRGDefinition",StringType,true),
    new StructField("ProviderId", LongType, false),
    new StructField("ProviderName", StringType, true),
    new StructField("ProviderStreetAddress", StringType, false),
    new StructField("ProviderCity", StringType, false),
    new StructField("ProviderState", StringType, false),
    new StructField("ProviderZipCode", LongType, false),
    new StructField("HospitalReferralRegionDescription", StringType,true),
    new StructField("TotalDischarges", LongType, false),
    new StructField("AverageCoveredCharges", DoubleType, false),
    new StructField("AverageTotalPayments", DoubleType,false),
    new StructField("AverageMedicarePayments", DoubleType,false)))

  //Now load the data with above schema
  val Hospital_data = spark.read.format("csv")
    .option("header", "true")
    .schema(Manual_schema)
    .load("D:\\Lavanya\\inpatientCharges.csv").toDF()

   Hospital_data.registerTempTable("IPatientCharges") //Registering as temporary table IPatientCharges.
   println("Dataframe Registered as table !")
   Hospital_data.show()

   val data1 = spark.sql("select ProviderState, avg(AverageCoveredCharges) from IPatientCharges group by ProviderState ")
   data1.show()

   val data2 = spark.sql("select ProviderState, avg(AverageTotalPayments) from IPatientCharges group by ProviderState ")
   data2.show()

   val data3 = spark.sql("select ProviderState, avg(AverageMedicarePayments) from IPatientCharges group by ProviderState ")
   data3.show()

   val data4 = spark.sql("select ProviderState, DRGDefinition, sum(TotalDischarges) as TotalDischarge from IPatientCharges group by ProviderState, DRGDefinition order by TotalDischarge desc")
   data4.show()
 }
}

