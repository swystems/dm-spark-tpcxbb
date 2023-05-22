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
-- Cluster customers into book buddies/club groups based on their in
-- store book purchasing histories. After model of separation is build, 
-- report for the analysed customers to which "group" they where assigned

-- IMPLEMENTATION NOTICE:
-- hive provides the input for the clustering program
-- The input format for the clustering is:
--   customer ID, 
--   sum of store sales in the item class ids [1,15]


-- This query requires parallel order by for fast and deterministic global ordering of final result
set hive.optimize.sampling.orderby=${bigbench.spark.sql.optimize.sampling.orderby};
set hive.optimize.sampling.orderby.number=${bigbench.spark.sql.optimize.sampling.orderby.number};
set hive.optimize.sampling.orderby.percent=${bigbench.spark.sql.optimize.sampling.orderby.percent};
--debug print
set hive.optimize.sampling.orderby;
set hive.optimize.sampling.orderby.number;
set hive.optimize.sampling.orderby.percent;

--ML-algorithms expect double values as input for their Vectors. 
DROP TABLE IF EXISTS ${TEMP_TABLE};
CREATE TABLE ${TEMP_TABLE} (
  cid  BIGINT,--used as "label", all following values are used as Vector for ML-algorithm
  id1  double,
  id2  double,
  id3  double,
  id4  double,
  id5  double,
  id6  double,
  id7  double,
  id8  double,
  id9  double,
  id10 double,
  id11 double,
  id12 double,
  id13 double,
  id14 double,
  id15 double
);

INSERT INTO TABLE ${TEMP_TABLE}
SELECT
  ss.ss_customer_sk AS cid,
  count(CASE WHEN i.i_class_id=1  THEN 1 ELSE NULL END) AS id1,
  count(CASE WHEN i.i_class_id=2  THEN 1 ELSE NULL END) AS id2,
  count(CASE WHEN i.i_class_id=3  THEN 1 ELSE NULL END) AS id3,
  count(CASE WHEN i.i_class_id=4  THEN 1 ELSE NULL END) AS id4,
  count(CASE WHEN i.i_class_id=5  THEN 1 ELSE NULL END) AS id5,
  count(CASE WHEN i.i_class_id=6  THEN 1 ELSE NULL END) AS id6,
  count(CASE WHEN i.i_class_id=7  THEN 1 ELSE NULL END) AS id7,
  count(CASE WHEN i.i_class_id=8  THEN 1 ELSE NULL END) AS id8,
  count(CASE WHEN i.i_class_id=9  THEN 1 ELSE NULL END) AS id9,
  count(CASE WHEN i.i_class_id=10 THEN 1 ELSE NULL END) AS id10,
  count(CASE WHEN i.i_class_id=11 THEN 1 ELSE NULL END) AS id11,
  count(CASE WHEN i.i_class_id=12 THEN 1 ELSE NULL END) AS id12,
  count(CASE WHEN i.i_class_id=13 THEN 1 ELSE NULL END) AS id13,
  count(CASE WHEN i.i_class_id=14 THEN 1 ELSE NULL END) AS id14,
  count(CASE WHEN i.i_class_id=15 THEN 1 ELSE NULL END) AS id15
FROM store_sales ss
INNER JOIN item i 
  ON (ss.ss_item_sk = i.i_item_sk
  AND i.i_category IN (${q26_i_category_IN})
  AND ss.ss_customer_sk IS NOT NULL
)
GROUP BY ss.ss_customer_sk
HAVING count(ss.ss_item_sk) > ${q26_count_ss_item_sk}
--CLUSTER BY cid --cluster by preceded by group by is silently ignored by hive but fails in spark
ORDER BY cid
;

-------------------------------------------------------------------------------
