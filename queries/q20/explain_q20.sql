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


-- TASK:
-- Customer segmentation for return analysis: Customers are separated
-- along the following dimensions: return frequency, return order ratio (total
-- number of orders partially or fully returned versus the total number of orders),
-- return item ratio (total number of items returned versus the number of items
-- purchased), return amount ration (total monetary amount of items returned versus
-- the amount purchased), return order ratio. Consider the store returns during
-- a given year for the computation.

-- IMPLEMENTATION NOTICE:
-- hive provides the input for the clustering program
-- The input format for the clustering is:
--   user surrogate key, 
--   order ratio (number of returns / number of orders), 
--   item ratio (number of returned items / number of ordered items), 
--   money ratio (returned money / payed money), 
--   number of returns


-- Resources


-- This query requires parallel order by for fast and deterministic global ordering of final result
set hive.optimize.sampling.orderby=${bigbench.spark.sql.optimize.sampling.orderby};
set hive.optimize.sampling.orderby.number=${bigbench.spark.sql.optimize.sampling.orderby.number};
set hive.optimize.sampling.orderby.percent=${bigbench.spark.sql.optimize.sampling.orderby.percent};
--debug print
set hive.optimize.sampling.orderby;
set hive.optimize.sampling.orderby.number;
set hive.optimize.sampling.orderby.percent;

EXPLAIN  
SELECT
  ss_customer_sk AS user_sk,
  round(CASE WHEN ((returns_count IS NULL) OR (orders_count IS NULL) OR ((returns_count / orders_count) IS NULL) ) THEN 0.0 ELSE (returns_count / orders_count) END, 7) AS orderRatio,
  round(CASE WHEN ((returns_items IS NULL) OR (orders_items IS NULL) OR ((returns_items / orders_items) IS NULL) ) THEN 0.0 ELSE (returns_items / orders_items) END, 7) AS itemsRatio,
  round(CASE WHEN ((returns_money IS NULL) OR (orders_money IS NULL) OR ((returns_money / orders_money) IS NULL) ) THEN 0.0 ELSE (returns_money / orders_money) END, 7) AS monetaryRatio,
  round(CASE WHEN ( returns_count IS NULL                                                                        ) THEN 0.0 ELSE  returns_count                 END, 0) AS frequency
FROM
  (
    SELECT
      ss_customer_sk,
      -- return order ratio
      COUNT(distinct(ss_ticket_number)) AS orders_count,
      -- return ss_item_sk ratio
      COUNT(ss_item_sk) AS orders_items,
      -- return monetary amount ratio
      SUM( ss_net_paid ) AS orders_money
    FROM store_sales s
    GROUP BY ss_customer_sk
  ) orders
  LEFT OUTER JOIN
  (
    SELECT
      sr_customer_sk,
      -- return order ratio
      count(distinct(sr_ticket_number)) as returns_count,
      -- return ss_item_sk ratio
      COUNT(sr_item_sk) as returns_items,
      -- return monetary amount ratio
      SUM( sr_return_amt ) AS returns_money
    FROM store_returns
    GROUP BY sr_customer_sk
  ) returned ON ss_customer_sk=sr_customer_sk
ORDER BY user_sk
;
