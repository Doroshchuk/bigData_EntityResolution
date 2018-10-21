import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EntityResolution {
    public static void main(String[] args){
        System.setProperty("hadoop.home.dir", "");
        final SparkSession sparkSession =  SparkSession.builder()
                .appName("Spark EntityResolution")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "./assets/")
                .getOrCreate();

        final DataFrameReader dataFrameReader = sparkSession.read()
                                    .option("header", "true")
                                    .option("inferSema", "true");
        final Dataset<Row> abtDataFrame = dataFrameReader.csv("./assets/Abt.csv");
        abtDataFrame.show();
        abtDataFrame.printSchema();
    }
}
