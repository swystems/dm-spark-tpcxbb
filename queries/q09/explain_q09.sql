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


-- Aggregate total amount of sold items over different given types of combinations of customers based on selected groups of
-- marital status, education status, sales price  and   different combinations of state and sales profit.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

-- the real query part
EXPLAIN 
SELECT SUM(ss1.ss_quantity)
FROM store_sales ss1, date_dim dd,customer_address ca1 , store s ,customer_demographics cd
-- select date range
WHERE ss1.ss_sold_date_sk = dd.d_date_sk 
AND dd.d_year=${q09_year}
AND ss1.ss_addr_sk = ca1.ca_address_sk
AND s.s_store_sk = ss1.ss_store_sk
AND cd.cd_demo_sk = ss1.ss_cdemo_sk
AND 
(
  (
    cd.cd_marital_status = '${q09_part1_marital_status}'
    AND cd.cd_education_status = '${q09_part1_education_status}'
    AND ${q09_part1_sales_price_min} <= ss1.ss_sales_price
    AND ss1.ss_sales_price <= ${q09_part1_sales_price_max}
  ) 
  OR 
  (
    cd.cd_marital_status = '${q09_part2_marital_status}'
    AND cd.cd_education_status = '${q09_part2_education_status}'
    AND ${q09_part2_sales_price_min} <= ss1.ss_sales_price
    AND ss1.ss_sales_price <= ${q09_part2_sales_price_max}
  ) 
  OR 
  (
    cd.cd_marital_status = '${q09_part3_marital_status}'
    AND cd.cd_education_status = '${q09_part3_education_status}'
    AND ${q09_part3_sales_price_min} <= ss1.ss_sales_price
    AND ss1.ss_sales_price <= ${q09_part3_sales_price_max}
  )
) 
AND 
(
  (
    ca1.ca_country = '${q09_part1_ca_country}'
    AND ca1.ca_state IN (${q09_part1_ca_state_IN})
    AND ${q09_part1_net_profit_min} <= ss1.ss_net_profit
    AND ss1.ss_net_profit <= ${q09_part1_net_profit_max}
  ) 
  OR 
  (
    ca1.ca_country = '${q09_part2_ca_country}'
    AND ca1.ca_state IN (${q09_part2_ca_state_IN})
    AND ${q09_part2_net_profit_min} <= ss1.ss_net_profit
    AND ss1.ss_net_profit <= ${q09_part2_net_profit_max}
  ) 
  OR 
  (
    ca1.ca_country = '${q09_part3_ca_country}'
    AND ca1.ca_state IN (${q09_part3_ca_state_IN})
    AND ${q09_part3_net_profit_min} <= ss1.ss_net_profit
    AND ss1.ss_net_profit <= ${q09_part3_net_profit_max}
  )
)
--no sorting required. output is a single line
;
