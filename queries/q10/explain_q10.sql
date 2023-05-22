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


-- For all products, extract sentences from its product reviews that contain positive or negative sentiment
-- and display for each item the sentiment polarity of the extracted sentences (POS OR NEG)
-- and the sentence and word in sentence leading to this classification

-- Resources
ADD JAR ${env:BIG_BENCH_QUERY_RESOURCES}/opennlp-maxent-3.0.3.jar;
ADD JAR ${env:BIG_BENCH_QUERY_RESOURCES}/opennlp-tools-1.9.3.jar;
ADD JAR ${env:BIG_BENCH_QUERY_RESOURCES}/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION extract_sentiment AS 'io.bigdatabenchmark.v1.queries.q10.SentimentUDF';

-- Query parameters

--Result  --------------------------------------------------------------------
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



-- the real query part
-- you may want to adapt: set hive.exec.reducers.bytes.per.reducer=xxxx;  Default Value: 1,000,000,000 prior to Hive 0.14.0; 256 MB (256,000,000) in Hive 0.14.0 and later

EXPLAIN 
SELECT item_sk, review_sentence, sentiment, sentiment_word
FROM (--wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
  SELECT extract_sentiment(pr_item_sk, pr_review_content) AS (item_sk, review_sentence, sentiment, sentiment_word)
  FROM product_reviews
) extracted
ORDER BY item_sk,review_sentence,sentiment,sentiment_word
--CLUSTER BY instead of ORDER BY does not work to achieve global ordering. e.g. 2 reducers: first reducer will write keys 0,2,4,6.. into file 000000_0 and reducer 2 will write keys 1,3,5,7,.. into file 000000_1.concatenating these files does not produces a deterministic result if number of reducer changes.
--Solution: parallel "order by" as non parallel version only uses a single reducer and we cant use "limit
;
