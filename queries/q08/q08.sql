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


--For online sales, compare the total sales monetary amount in which customers checked
--online reviews 3 days before making the purchase and that of sales in which customers
--did not read reviews. Consider only online sales for a specific category in a given
--year.

-- Resources
ADD FILE ${QUERY_DIR}/q08_filter_sales_with_reviews_viewed_before.py;

DROP TABLE IF EXISTS ${TEMP_TABLE2};
DROP TABLE IF EXISTS ${TEMP_TABLE3};
DROP TABLE IF EXISTS ${TEMP_TABLE1};


--DateFilter
CREATE TABLE ${TEMP_TABLE1} AS
SELECT d_date_sk
FROM date_dim d
WHERE d.d_date >= '${q08_startDate}'
AND   d.d_date <= '${q08_endDate}'
;

--PART 1 - sales_sk's that users have visited a review page before in a given time period --------------------------------------------------------
CREATE TABLE ${TEMP_TABLE2} AS
SELECT DISTINCT wcs_sales_sk
FROM ( -- sessionize clickstreams and filter "viewed reviews" by looking at the web_page page type using a python script
  FROM ( -- select only webclicks in relevant time frame and get the type
    SELECT  wcs_user_sk,
            (wcs_click_date_sk * 86400L + wcs_click_time_sk) AS tstamp_inSec, --every wcs_click_date_sk equals one day => convert to seconds date*24*60*60=date*86400 and add time_sk
            wcs_sales_sk,
            wp_type          
    FROM web_clickstreams 
    LEFT SEMI JOIN ${TEMP_TABLE1} date_filter ON (wcs_click_date_sk = date_filter.d_date_sk and wcs_user_sk IS NOT NULL)
    JOIN web_page w ON wcs_web_page_sk = w.wp_web_page_sk
    --WHERE wcs_user_sk IS NOT NULL
    DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk,tstamp_inSec,wcs_sales_sk,wp_type -- cluster by uid and sort by tstamp required by following python script
  ) q08_map_output
  -- input: web_clicks in a given year
  REDUCE  wcs_user_sk,
          tstamp_inSec,
          wcs_sales_sk,
          wp_type
  USING 'python3 q08_filter_sales_with_reviews_viewed_before.py review ${q08_seconds_before_purchase}'
  AS (wcs_sales_sk BIGINT)
) sales_which_read_reviews
;


--PART 2 - helper table: sales within one year starting 1999-09-02  ---------------------------------------
--DROP TABLE IF EXISTS ${TEMP_TABLE3};
CREATE TABLE IF NOT EXISTS ${TEMP_TABLE3} AS
SELECT ws_net_paid, ws_order_number
FROM web_sales ws
JOIN ${TEMP_TABLE1} d ON ( ws.ws_sold_date_sk = d.d_date_sk)
;


--PART 3 - for sales in given year, compute sales in which customers checked online reviews vs. sales in which customers did not read reviews.
--Result --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${RESULT_TABLE};
CREATE TABLE ${RESULT_TABLE} (
  q08_review_sales_amount    BIGINT,
  no_q08_review_sales_amount BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_spark_sql_default_fileformat_result_table} LOCATION '${RESULT_DIR}';

-- the real query part----------------------------------------------------------------------
INSERT INTO TABLE ${RESULT_TABLE}
SELECT
  q08_review_sales.amount AS q08_review_sales_amount,
  q08_all_sales.amount - q08_review_sales.amount AS no_q08_review_sales_amount
-- both subqueries only contain a single line with the aggregated sum. Join on 1=1 to get both results into same line for calculating the difference of the two results
FROM (
  SELECT 1 AS id, SUM(ws_net_paid) as amount
  FROM ${TEMP_TABLE3} allSalesInYear
  LEFT SEMI JOIN ${TEMP_TABLE2} salesWithViewedReviews ON allSalesInYear.ws_order_number = salesWithViewedReviews.wcs_sales_sk
) q08_review_sales
JOIN (
  SELECT 1 AS id, SUM(ws_net_paid) as amount
  FROM ${TEMP_TABLE3} allSalesInYear
) q08_all_sales
ON q08_review_sales.id = q08_all_sales.id
--result is one single line, no sorting required
;


--cleanup-------------------------------------------------------------------
DROP TABLE IF EXISTS ${TEMP_TABLE2};
DROP TABLE IF EXISTS ${TEMP_TABLE3};
DROP TABLE IF EXISTS ${TEMP_TABLE1};
