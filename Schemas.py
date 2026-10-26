# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS supplychain.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS supplychain.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS supplychain.gold;
# MAGIC