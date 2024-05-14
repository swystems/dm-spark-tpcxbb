--
-- Copyright (C) 2019 Transaction Processing Performance Council (TPC) and/or its contributors.
-- This file is part of a software package distributed by the TPC
-- The contents of this file have been developed by the TPC, and/or have been licensed to the TPC under one or more contributor
-- license agreements.
-- This file is subject to the terms and conditions outlined in the End-User
-- License Agreement (EULA) which can be found in this distribution (EULA.txt) and is available at the following URL:
-- http://www.tpc.org/TPC_Documents_Current_Versions/txt/EULA.txt
-- Unless required by applicable law or agreed to in writing, this software is distributed on an "AS IS" BASIS, WITHOUT
-- WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, and the user bears the entire risk as to quality
-- and performance as well as the entire cost of service or repair in case of defect. See the EULA for more details.
-- 
--


--
-- Copyright 2015-2019 Intel Corporation.
-- This software and the related documents are Intel copyrighted materials, and your use of them 
-- is governed by the express license under which they were provided to you ("License"). Unless the 
-- License provides otherwise, you may not use, modify, copy, publish, distribute, disclose or 
-- transmit this software or the related documents without Intel's prior written permission.
-- 
-- This software and the related documents are provided as is, with no express or implied warranties, 
-- other than those that are expressly stated in the License.
-- 
--


-- TASK: (Based, but not equal to tpc-ds q6)
-- List top 10 states in descending order with at least 10 customers who during
-- a given month bought products with the price tag at least 20% higher than the
-- average price of products in the same category.


-- helper table: items with 20% higher then avg prices of product from same category
DROP TABLE IF EXISTS ${TEMP_TABLE};
CREATE TABLE ${TEMP_TABLE} AS
-- "price tag at least 20% higher than the average price of products in the same category."
SELECT k.i_item_sk
FROM item k,
(
  SELECT
    i_category,
    AVG(j.i_current_price) * ${q07_HIGHER_PRICE_RATIO} AS avg_price
  FROM item j
  GROUP BY j.i_category
) avgCategoryPrice
WHERE avgCategoryPrice.i_category = k.i_category
AND k.i_current_price > avgCategoryPrice.avg_price
;


--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${RESULT_TABLE};
CREATE TABLE ${RESULT_TABLE} (
  ca_state STRING,
  cnt      BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_spark_sql_default_fileformat_result_table} LOCATION '${RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${RESULT_TABLE}
SELECT
  ca_state,
  COUNT(*) AS cnt
FROM
  customer_address a,
  customer c,
  store_sales s,
  ${TEMP_TABLE} highPriceItems -- defined above
WHERE a.ca_address_sk = c.c_current_addr_sk
AND c.c_customer_sk = s.ss_customer_sk
AND ca_state IS NOT NULL
AND ss_item_sk = highPriceItems.i_item_sk --cannot use "ss_item_sk IN ()". Hive only supports a single "IN" subquery per SQL statement.
AND s.ss_sold_date_sk IN
( --during a given month
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = ${q07_YEAR}
  AND d_moy = ${q07_MONTH}
)
GROUP BY ca_state
HAVING cnt >= ${q07_HAVING_COUNT_GE} --at least 10 customers
ORDER BY cnt DESC, ca_state --top 10 states in descending order 
LIMIT ${q07_LIMIT}
;


--cleanup
DROP TABLE IF EXISTS ${TEMP_TABLE};
