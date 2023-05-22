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


-- based on tpc-ds q39
-- This query contains multiple, related iterations:
-- Iteration 1: Calculate the coefficient of variation and mean of every item
-- and warehouse of the given and the consecutive month
-- Iteration 2: Find items that had a coefficient of variation of 1.3 or larger
-- in the given and the consecutive month


DROP TABLE IF EXISTS ${TEMP_TABLE};
CREATE TABLE ${TEMP_TABLE} AS
SELECT
  inv_warehouse_sk,
 -- w_warehouse_name,
  inv_item_sk,
  d_moy,
  cast( ( stdev / mean ) as decimal(15,5)) cov
FROM (
   --Iteration 1: Calculate the coefficient of variation and mean of every item
   -- and warehouse of the given and the consecutive month
  SELECT
    inv_warehouse_sk,
    inv_item_sk,
    d_moy,
    -- implicit group by d_moy using CASE filters inside the stddev_samp() and avg() UDF's. This saves us from requiring a self join for correlation of d_moy and d_moy+1 later on.
    cast( stddev_samp( inv_quantity_on_hand ) as decimal(15,5)) stdev,
    cast(         avg( inv_quantity_on_hand ) as decimal(15,5)) mean
   
  FROM inventory inv
  JOIN date_dim d 
       ON (inv.inv_date_sk = d.d_date_sk
       AND d.d_year = ${q23_year} 
       AND d_moy between ${q23_month} AND (${q23_month} + 1) 
       )
  GROUP BY
    inv_warehouse_sk,
    inv_item_sk,
    d_moy
) q23_tmp_inv_part
--JOIN warehouse w ON inv_warehouse_sk = w.w_warehouse_sk
WHERE mean > 0 --avoid "div by 0"
  AND stdev/mean >= ${q23_coefficient} 
;


--- RESULT --------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

-- This query requires parallel order by for fast and deterministic global ordering of final result
set hive.optimize.sampling.orderby=${bigbench.spark.sql.optimize.sampling.orderby};
set hive.optimize.sampling.orderby.number=${bigbench.spark.sql.optimize.sampling.orderby.number};
set hive.optimize.sampling.orderby.percent=${bigbench.spark.sql.optimize.sampling.orderby.percent};
--debug print
set hive.optimize.sampling.orderby;
set hive.optimize.sampling.orderby.number;
set hive.optimize.sampling.orderby.percent;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${RESULT_TABLE};
CREATE TABLE ${RESULT_TABLE} (
  inv_warehouse_sk     BIGINT,
  inv_item_sk          BIGINT,
  d_moy                int,
  cov                  decimal(15,5),
  d_moy_consecutive    int,
  cov_consecutive      decimal(15,5)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_spark_sql_default_fileformat_result_table} LOCATION '${RESULT_DIR}';

-- Begin: the real query part
INSERT INTO TABLE ${RESULT_TABLE}
-- Iteration 2: Find items that had a coefficient of variation of 1.5 or larger
-- in the given and the consecutive month
SELECT 
  inv1.inv_warehouse_sk,
  inv1.inv_item_sk,
  inv1.d_moy,
  inv1.cov,
  inv2.d_moy,
  inv2.cov
FROM ${TEMP_TABLE} inv1
JOIN ${TEMP_TABLE} inv2 
    ON(   inv1.inv_warehouse_sk=inv2.inv_warehouse_sk
      AND inv1.inv_item_sk =  inv2.inv_item_sk
      AND inv1.d_moy = ${q23_month}
      AND inv2.d_moy = ${q23_month} + 1
    )
ORDER BY
 inv1.inv_warehouse_sk,
 inv1.inv_item_sk
;
  
DROP TABLE IF EXISTS ${TEMP_TABLE};

  
  