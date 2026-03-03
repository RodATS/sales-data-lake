from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, upper

def main():

    spark = (
        SparkSession.builder
        .appName("BronzeToSilverSales")
        .master("local[*]")
        .getOrCreate()
    )

    bronze_path = "data/bronze/sales"
    silver_path = "data/silver/sales"

    # Leer Bronze
    df = spark.read.parquet(bronze_path)

    print("Schema Bronze:")
    df.printSchema()

    # Transformaciones Silver
    df_silver = (
        df
        # eliminar columna técnica
        .drop("Row_ID")

        # convertir fechas
        .withColumn("order_date", to_date(col("Order_Date"), "M/d/yyyy"))
.withColumn("ship_date", to_date(col("Ship_Date"), "M/d/yyyy"))

        # normalizar texto
        .withColumn("customer_name", trim(col("Customer_Name")))
        .withColumn("segment", upper(col("Segment")))

        # manejar postal_code nulo
        .withColumn(
            "postal_code",
            col("Postal_Code").cast("string")
        )

        # renombrar columnas principales
        .withColumnRenamed("Order_ID", "order_id")
        .withColumnRenamed("Customer_ID", "customer_id")
        .withColumnRenamed("Product_ID", "product_id")
        .withColumnRenamed("Category", "category")
        .withColumnRenamed("Sub_Category", "sub_category")
        .withColumnRenamed("Product_Name", "product_name")
        .withColumnRenamed("Sales", "sales")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Region", "region")
        .withColumnRenamed("Country", "country")
    )

    print("Schema Silver:")
    df_silver.printSchema()

    # Escribir Silver particionado por año
    (
        df_silver
        .withColumn("year", col("order_date").substr(1,4))
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(silver_path)
    )

    print("Datos escritos en SILVER correctamente")

    spark.stop()

if __name__ == "__main__":
    main()