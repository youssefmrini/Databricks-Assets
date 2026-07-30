// Databricks notebook source

val xmlString = """
  <books>
    <book id="bk103">
      <author>Corets, Eva</author>
      <title>Maeve Ascendant</title>
    </book>
    <book id="bk104">
      <author>Corets, Eva</author>
      <title>Oberon's Legacy</title>
    </book>
  </books>"""
val xmlPath = "dbfs:/tmp/books.xml"
dbutils.fs.put(xmlPath, xmlString)

// COMMAND ----------


val df = spark.read.option("rowTag", "books").xml(xmlPath)
df.printSchema()
df.show(truncate=false)