#This is a practice:
#We are represented with 2 entity tables(customer, product) and 1 relation table(sales).
#We need to load them with pyspark and show the relationships from the fact table
#For the end, comapre the speed of doing it in Python vs SQL


from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DecimalType, DateType
import time #for measuring how much Python vs SQL needs time

spark = SparkSession.builder.appName("practice_test_1").getOrCreate()

#id, name, email
schemaCustomer = StructType([ 
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

#id, name, price(decimal)
schemaProduct = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("price", DecimalType(19,2), True)
])

#sale_id,customer_id,product_id,sale_date,quantity
schemaSales = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sale_date", DateType(), True),
    StructField("quantity", IntegerType(), True)
])

customer_csv_path = "C:/Users/Nikola/Desktop/practice1/customer.csv"
product_csv_path =  "C:/Users/Nikola/Desktop/practice1/product.csv"
sales_csv_path = "C:/Users/Nikola/Desktop/practice1/sales.csv"

dfCustomer = spark.read.option("header", True).schema(schemaCustomer).csv(customer_csv_path)
dfProduct = spark.read.option("header", True).schema(schemaProduct).csv(product_csv_path)
dfSales = spark.read.option("header", True).schema(schemaSales).csv(sales_csv_path)

start_time = time.time() #timer starts

#-------------------------------Python way---------------------------------------

dfResult = dfSales.join(dfCustomer, dfSales.customer_id == dfCustomer.id, "left")\
                  .join(dfProduct, dfSales.product_id == dfProduct.id, "left")\
.select(
    dfCustomer["name"].alias("customer_name"),
    dfProduct["name"].alias("product_name"),
    dfProduct["price"].alias("product_price"),
    dfSales["sale_date"],
    dfSales["quantity"]
)

#---------------------------------------------------------------------------------

#-------------------------------SQL way-------------------------------------------

# dfCustomer.createOrReplaceTempView("dim_customer")
# dfProduct.createOrReplaceTempView("dim_product")
# dfSales.createOrReplaceTempView("fact_sales")

# dfResult = spark.sql("""
#                         SELECT 
#                             f.sale_id, 
#                             c.name as customer_name,
#                             p.name as product_name,
#                             p.price as product_price, 
#                             f.sale_date, 
#                             f.quantity
#                         FROM fact_sales f
#                         LEFT JOIN dim_customer c ON f.customer_id = c.id
#                         LEFT JOIN dim_product p ON f.product_id = p.id
#                     """)

#---------------------------------------------------------------------------------

dfResult.show()

end_time = time.time() #timer ends

elapsed_time = end_time - start_time
print(f"Query execution time: {elapsed_time} seconds")

#Conclusion of this test based on 2 small categories of data:
#First category is with around 20 records in total and second is with around 300 records
#PySpark is faster by few milliseconds to half a second than SparkSQL, probably because of the additional step of creating views
#For this test to be real, I will need a few million records, further optimizations, and a cluster instead of a single laptop