// Databricks notebook source
val primedataRdd = sc.textFile("/FileStore/tables/primeNumber.text")

// COMMAND ----------

val primedata = primedataRdd.first()
val pd = primedataRdd.filter(row => row != primedata)
val pdMap = pd.map(x=>x.toInt)

def checkPrime(i:Int): Boolean = {
  if (i <= 1)
      false
  else if (i == 2 )
      true
  else
    !(2 to (i-1)).exists(x => i % x == 0)
  
}

val result = pdMap.map(checkPrime).collect()


// COMMAND ----------

val listData = List("Gurpreet 1998", "Gurkirat 1998", "Rajdeep 1999" , "Gill 1997" )

// COMMAND ----------

val datardd = listData.map(x=> (x.split(" ")(0),(x.split("")(1))))

// COMMAND ----------

val airportData = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val airportRdd = airportData.map(x => (x.split(",")(1), x.split(",")(11).toLowerCase))
airportRdd.take(3)

// COMMAND ----------


