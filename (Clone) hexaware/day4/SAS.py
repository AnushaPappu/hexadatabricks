# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.<storageaccountname>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storageaccountname>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storageaccountname>.dfs.core.windows.net","SASToken")
