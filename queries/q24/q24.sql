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


--For a given product, measure the effect of competitor's prices on
--products' in-store and online sales. (Compute the cross-price elasticity of demand
--for a given product.)
-- Step1 :
--Calculating the Percentage Change in Quantity Demanded of Good X : [QDemand(NEW) - QDemand(OLD)] / QDemand(OLD)
--Step 2:
-- Calculating the Percentage Change in Price of Good Y: [Price(NEW) - Price(OLD)] / Price(OLD)
-- Step 3 final:
--Cross-Price Elasticity of Demand (CPEoD) is given by: CPEoD = (% Change in Quantity Demand for Good X)/(% Change in Price for Good Y))




-- compute the price change % for the competitor items
-- will give a list of competitor prices changes
DROP TABLE IF EXISTS ${TEMP_TABLE};
CREATE TABLE ${TEMP_TABLE} AS
SELECT
  i_item_sk, 
  imp_sk,
  --imp_competitor,
  (imp_competitor_price - i_current_price)/i_current_price AS price_change,
  imp_start_date, 
  (imp_end_date - imp_start_date) AS no_days_comp_price
FROM item i ,item_marketprices imp 
WHERE i.i_item_sk = imp.imp_item_sk
AND i.i_item_sk = ${q24_i_item_sk}
-- AND imp.imp_competitor_price < i.i_current_price --consider all price changes not just where competitor is cheaper
ORDER BY i_item_sk, 
         imp_sk, 
         --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number 
         imp_start_date
;



--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${RESULT_TABLE};
CREATE TABLE ${RESULT_TABLE} (
  i_item_sk               BIGINT,
  --imp_competitor          STRING, --add to compute cross_price_elasticity per competitor is instead of a single number 
  cross_price_elasticity  decimal(15,7)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_spark_sql_default_fileformat_result_table} LOCATION '${RESULT_DIR}';

-- Begin: the real query part
INSERT INTO TABLE ${RESULT_TABLE}
SELECT ws_item_sk,
       --ws.imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number 
       avg ( (current_ss_quant + current_ws_quant - prev_ss_quant - prev_ws_quant) / ((prev_ss_quant + prev_ws_quant) * ws.price_change)) AS cross_price_elasticity
FROM
    ( --websales items sold quantity before and after competitor price change
      SELECT
        ws_item_sk,
        imp_sk,
        --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number 
        price_change,
        SUM( CASE WHEN  ( (ws_sold_date_sk >= c.imp_start_date) AND (ws_sold_date_sk < (c.imp_start_date + c.no_days_comp_price))) THEN ws_quantity ELSE 0 END ) AS current_ws_quant,
        SUM( CASE WHEN  ( (ws_sold_date_sk >= (c.imp_start_date - c.no_days_comp_price)) AND (ws_sold_date_sk < c.imp_start_date)) THEN ws_quantity ELSE 0 END ) AS prev_ws_quant
      FROM web_sales ws
      JOIN ${TEMP_TABLE} c ON ws.ws_item_sk = c.i_item_sk
      GROUP BY ws_item_sk, 
              imp_sk, 
              --imp_competitor,
              price_change
    ) ws
JOIN
    (--storesales items sold quantity before and after competitor price change
      SELECT
        ss_item_sk,
        imp_sk,
        --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number 
        price_change,
        SUM( CASE WHEN ((ss_sold_date_sk >= c.imp_start_date) AND (ss_sold_date_sk < (c.imp_start_date + c.no_days_comp_price))) THEN ss_quantity ELSE 0 END) AS current_ss_quant,
        SUM( CASE WHEN ((ss_sold_date_sk >= (c.imp_start_date - c.no_days_comp_price)) AND (ss_sold_date_sk < c.imp_start_date)) THEN ss_quantity ELSE 0 END) AS prev_ss_quant
      FROM store_sales ss
      JOIN ${TEMP_TABLE} c ON c.i_item_sk = ss.ss_item_sk
      GROUP BY ss_item_sk, 
              imp_sk, 
              --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number 
              price_change
    ) ss
 ON (ws.ws_item_sk = ss.ss_item_sk and ws.imp_sk = ss.imp_sk)
GROUP BY  ws.ws_item_sk
--uncomment below to compute cross_price_elasticity per competitor is instead of a single number (requires ordering)
         --,ws.imp_competitor 
--ORDER BY ws.ws_item_sk, 
--         ws.imp_competitor       
;


-- clean up -----------------------------------
DROP TABLE ${TEMP_TABLE};
