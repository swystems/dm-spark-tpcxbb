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


-- based on tpc-ds q40
-- Compute the impact of an item price change on the
-- store sales by computing the total sales for items in a 30 day period before and
-- after the price change. Group the items by location of warehouse where they
-- were delivered from.

-- Resources


set hive.exec.compress.output=false;
set hive.exec.compress.output;


-- the real query part
EXPLAIN 
SELECT w_state, i_item_id,
  SUM(
    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') < unix_timestamp('${q16_date}','yyyy-MM-dd'))
    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
    ELSE 0.0 END
  ) AS sales_before,
  SUM(
    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') >= unix_timestamp('${q16_date}','yyyy-MM-dd'))
    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
    ELSE 0.0 END
  ) AS sales_after
FROM (
  SELECT *
  FROM web_sales ws
  LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number
    AND ws.ws_item_sk = wr.wr_item_sk)
) a1
JOIN item i ON a1.ws_item_sk = i.i_item_sk
JOIN warehouse w ON a1.ws_warehouse_sk = w.w_warehouse_sk
JOIN date_dim d ON a1.ws_sold_date_sk = d.d_date_sk
AND unix_timestamp(d.d_date, 'yyyy-MM-dd') >= unix_timestamp('${q16_date}', 'yyyy-MM-dd') - 30*24*60*60 --subtract 30 days in seconds
AND unix_timestamp(d.d_date, 'yyyy-MM-dd') <= unix_timestamp('${q16_date}', 'yyyy-MM-dd') + 30*24*60*60 --add 30 days in seconds
GROUP BY w_state,i_item_id
--original was ORDER BY w_state,i_item_id , but CLUSTER BY is hives cluster scale counter part
ORDER BY w_state,i_item_id
LIMIT 100
;

-- cleaning up ---------------------------------------------------------------------
