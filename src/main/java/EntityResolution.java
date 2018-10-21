import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class EntityResolution {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "/Users/m/Desktop/hadoop/hadoop-2.7.5/bin/hadoop");
        final SparkSession sparkSession = buildSparkSession();
        final DataFrameReader dataFrameReader = sparkSession.read().option("header", "true");

        final Dataset<Row> abtProductsWithPrices = renameSchema(getProductsWithPrice(dataFrameReader, "./assets/Abt.csv"), "Abt");
        final Dataset<Row> buyProductsWithPrices = renameSchema(getProductsWithPrice(dataFrameReader, "./assets/Buy.csv"), "Buy");

        final Dataset<Row> productsDuplicates = findDuplicatesId(buyProductsWithPrices, abtProductsWithPrices);
        productsDuplicates.printSchema();
        System.out.println("Count:" + productsDuplicates.count());
        productsDuplicates.write().csv("./assets/result");
    }

    private static Dataset<Row> findDuplicatesId(Dataset<Row> buySet, Dataset<Row> abtSet) {
        final Integer fuzzySearchSensibility = 27;
        return buySet.join(
                abtSet,
                levenshtein(
                        buySet.col("nameBuy"),
                        abtSet.col("nameAbt")
                ).$less(fuzzySearchSensibility)
        ).select("idBuy", "idAbt");
    }

    private static SparkSession buildSparkSession() {
        return SparkSession.builder()
                .appName("Spark EntityResolution")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "./assets/")
                .getOrCreate();
    }

    private static Dataset<Row> getProductsWithPrice(DataFrameReader dataFrameReader, String csvPath) {
        final Dataset<Row> buyDataFrame = dataFrameReader.csv(csvPath);
        return buyDataFrame.filter(buyDataFrame.col("price").notEqual("null"));
    }

    private static Dataset<Row> renameSchema(Dataset<Row> set, String schemaSuffix) {
        return set.withColumnRenamed("id", "id" + schemaSuffix)
                .withColumnRenamed("name", "name" + schemaSuffix)
                .withColumnRenamed("description", "description" + schemaSuffix)
                .withColumnRenamed("price", "price" + schemaSuffix);
    }

}
