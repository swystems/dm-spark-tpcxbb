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


-- based on tpc-ds q61
-- Find the ratio of items sold with and without promotions
-- in a given month and year. Only items in certain categories sold to customers
-- living in a specific time zone are considered.

-- Resources


--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${RESULT_TABLE};
CREATE TABLE ${RESULT_TABLE} (
  promotions decimal(15,2),
  total      decimal(15,2),
  cnt        decimal(15,2)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_spark_sql_default_fileformat_result_table} LOCATION '${RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${RESULT_TABLE}
-- no need to cast promotions or total to double: SUM(COL) already returned a DOUBLE
SELECT sum(promotional) as promotional, sum(total) as total,
       CASE WHEN sum(total) > 0 THEN 100*sum(promotional)/sum(total)
                                ELSE 0.0 END as promo_percent
FROM(
SELECT p_channel_email, p_channel_dmail, p_channel_tv,
CASE WHEN (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
THEN SUM(ss_ext_sales_price) ELSE 0 END as promotional,
SUM(ss_ext_sales_price) total
  FROM store_sales ss
  LEFT SEMI JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk AND dd.d_year = ${q17_year} AND dd.d_moy = ${q17_month}
  LEFT SEMI JOIN item i ON ss.ss_item_sk = i.i_item_sk AND i.i_category IN (${q17_i_category_IN})
  LEFT SEMI JOIN store s ON ss.ss_store_sk = s.s_store_sk AND s.s_gmt_offset = ${q17_gmt_offset}
  LEFT SEMI JOIN ( SELECT c.c_customer_sk FROM customer c LEFT SEMI JOIN customer_address ca
                   ON c.c_current_addr_sk = ca.ca_address_sk AND ca.ca_gmt_offset = ${q17_gmt_offset}
                 ) sub_c ON ss.ss_customer_sk = sub_c.c_customer_sk
  JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
  GROUP BY p_channel_email, p_channel_dmail, p_channel_tv
  ) sum_promotional
-- we don't need a 'ON' join condition. result is just two numbers.
ORDER by promotional, total
LIMIT 100 -- kinda useless, result is one line with two numbers, but original tpc-ds query has it too.
;
