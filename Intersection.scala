// Databricks notebook source
val nasaJuly = sc.textFile("/FileStore/tables/nasa_july.tsv")
val nasaAug = sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val hostJuly = nasaJuly.map(x => x.split("\t")(0))
val hostAug = nasaAug.map(x => x.split("\t")(0))

// COMMAND ----------

var intersection = hostJuly.intersection(hostAug)

// COMMAND ----------

def removeHeader(line:String): Boolean= !(line.startsWith("host"))
intersection.filter(x=>removeHeader(x))count()

// COMMAND ----------


