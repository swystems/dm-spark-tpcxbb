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


-- Retrieve the items with the highest number of returns where the number
-- of returns was approximately equivalent across all store and web channels
-- (within a tolerance of +/- 10%), within the week ending given dates. Analyse
-- the online reviews for these items to see if there are any major negative reviews.


ADD JAR ${env:BIG_BENCH_QUERY_RESOURCES}/opennlp-maxent-3.0.3.jar;
ADD JAR ${env:BIG_BENCH_QUERY_RESOURCES}/opennlp-tools-1.9.3.jar;
ADD JAR ${env:BIG_BENCH_QUERY_RESOURCES}/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION extract_sentiment AS 'io.bigdatabenchmark.v1.queries.q10.SentimentUDF';

--Result  returned items with negative sentiment --------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

-- This query requires parallel orderby for fast and deterministic global ordering of final result
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
  item_sk         BIGINT,
  review_sentence STRING,
  sentiment       STRING,
  sentiment_word  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_spark_sql_default_fileformat_result_table} LOCATION '${RESULT_DIR}';

---- the real query --------------
INSERT INTO TABLE ${RESULT_TABLE}
SELECT *
FROM
( --wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
  SELECT extract_sentiment(pr.pr_item_sk, pr.pr_review_content) AS
  (
    item_sk,
    review_sentence,
    sentiment,
    sentiment_word
  )
  FROM product_reviews pr,
  (
    --store returns in week ending given date
    SELECT sr_item_sk, SUM(sr_return_quantity) sr_item_qty
    FROM store_returns sr,
    (
      -- within the week ending a given date
      SELECT d1.d_date_sk
      FROM date_dim d1, date_dim d2
      WHERE d1.d_week_seq = d2.d_week_seq
      AND d2.d_date IN ( ${q19_storeReturns_date_IN} )
    ) sr_dateFilter
    WHERE sr.sr_returned_date_sk = d_date_sk
    GROUP BY sr_item_sk --across all store and web channels
    HAVING sr_item_qty > 0
  ) fsr,
  (
    --web returns in week ending given date
    SELECT wr_item_sk, SUM(wr_return_quantity) wr_item_qty
    FROM web_returns wr,
    (
      -- within the week ending a given date
      SELECT d1.d_date_sk
      FROM date_dim d1, date_dim d2
      WHERE d1.d_week_seq = d2.d_week_seq
      AND d2.d_date IN ( ${q19_webReturns_date_IN} )
    ) wr_dateFilter
    WHERE wr.wr_returned_date_sk = d_date_sk
    GROUP BY wr_item_sk  --across all store and web channels
    HAVING wr_item_qty > 0
  ) fwr
  WHERE fsr.sr_item_sk = fwr.wr_item_sk
  AND pr.pr_item_sk = fsr.sr_item_sk --extract product_reviews for found items
  -- equivalent across all store and web channels (within a tolerance of +/- 10%)
  AND abs( (sr_item_qty-wr_item_qty)/ ((sr_item_qty+wr_item_qty)/2)) <= 0.1
)extractedSentiments
WHERE sentiment= 'NEG' --if there are any major negative reviews.
--item_sk is skewed, but we need to sort by it. Technically we just expect a deterministic global sorting and not clustering by item_sk...so we could distribute by pr_review_sk
ORDER BY item_sk,review_sentence,sentiment,sentiment_word
;
