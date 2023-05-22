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


-- TASK
-- Build text classifier for online review sentiment classification (Positive,
-- Negative, Neutral), using 90% of available reviews for training and the remaining
-- 40% for testing. Display classifier accuracy on testing data 
-- and classification result for the 10% testing data: <reviewSK>,<originalRating>,<classificationResult>



-- This query requires parallel order by for fast and deterministic global ordering of final result
set hive.optimize.sampling.orderby=${bigbench.spark.sql.optimize.sampling.orderby};
set hive.optimize.sampling.orderby.number=${bigbench.spark.sql.optimize.sampling.orderby.number};
set hive.optimize.sampling.orderby.percent=${bigbench.spark.sql.optimize.sampling.orderby.percent};
--debug print
set hive.optimize.sampling.orderby;
set hive.optimize.sampling.orderby.number;
set hive.optimize.sampling.orderby.percent;


--Result 1 Training table for classification
DROP TABLE IF EXISTS ${TEMP_TABLE1};
CREATE TABLE ${TEMP_TABLE1} (
  pr_review_sk      BIGINT,
  pr_rating         INT,
  pr_review_content STRING
);

--Result 2 Testing table for classification
DROP TABLE IF EXISTS ${TEMP_TABLE2};
CREATE TABLE ${TEMP_TABLE2} (
  pr_review_sk      BIGINT,
  pr_rating         INT,
  pr_review_content STRING
);

--Split reviews table into training and testing
EXPLAIN 
FROM (
  SELECT
    pr_review_sk,
    pr_review_rating,
    pr_review_content
  FROM product_reviews
  ORDER BY pr_review_sk
)p
INSERT OVERWRITE TABLE ${TEMP_TABLE1}
  SELECT *
  WHERE pmod(pr_review_sk, 10) IN (1,2,3,4,5,6,7,8,9) -- 90% are training
INSERT OVERWRITE TABLE ${TEMP_TABLE2}
  SELECT *
  WHERE pmod(pr_review_sk, 10) IN (0) -- 10% are testing
;
