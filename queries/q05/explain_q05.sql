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
-- Build a model using logistic regression for a visitor to an online store: based on existing users online
-- activities (interest in items of different categories) and demographics.
-- This model will be used to predict if the visitor is interested in a given item category.
-- Output the precision, accuracy and confusion matrix of model.
-- Note: no need to actually classify existing users, as it will be later used to predict interests of unknown visitors.

-- input vectors to the machine learning algorithm are:
--  clicks_in_category BIGINT, -- used as label - number of clicks in specified category "q05_i_category"
--  college_education  BIGINT, -- has college education [0,1]
--  male               BIGINT, -- isMale [0,1]
--  clicks_in_1        BIGINT, -- number of clicks in category id 1
--  clicks_in_2        BIGINT, -- number of clicks in category id 2
--  clicks_in_3        BIGINT, -- number of clicks in category id 3
--  clicks_in_4        BIGINT, -- number of clicks in category id 4
--  clicks_in_5        BIGINT, -- number of clicks in category id 5
--  clicks_in_6        BIGINT  -- number of clicks in category id 6
--  clicks_in_7        BIGINT  -- number of clicks in category id 7


EXPLAIN 
SELECT
  --wcs_user_sk,
  clicks_in_category,
  CASE WHEN cd_education_status IN (${q05_cd_education_status_IN}) THEN 1 ELSE 0 END AS college_education,
  CASE WHEN cd_gender = ${q05_cd_gender} THEN 1 ELSE 0 END AS male,
  clicks_in_1,
  clicks_in_2,
  clicks_in_3,
  clicks_in_4,
  clicks_in_5,
  clicks_in_6,
  clicks_in_7
FROM( 
  SELECT 
    wcs_user_sk,
    SUM( CASE WHEN i_category = ${q05_i_category} THEN 1 ELSE 0 END) AS clicks_in_category,
    SUM( CASE WHEN i_category_id = 1 THEN 1 ELSE 0 END) AS clicks_in_1,
    SUM( CASE WHEN i_category_id = 2 THEN 1 ELSE 0 END) AS clicks_in_2,
    SUM( CASE WHEN i_category_id = 3 THEN 1 ELSE 0 END) AS clicks_in_3,
    SUM( CASE WHEN i_category_id = 4 THEN 1 ELSE 0 END) AS clicks_in_4,
    SUM( CASE WHEN i_category_id = 5 THEN 1 ELSE 0 END) AS clicks_in_5,
    SUM( CASE WHEN i_category_id = 6 THEN 1 ELSE 0 END) AS clicks_in_6,
    SUM( CASE WHEN i_category_id = 7 THEN 1 ELSE 0 END) AS clicks_in_7
  FROM web_clickstreams
  INNER JOIN item it ON (wcs_item_sk = i_item_sk
                     AND wcs_user_sk IS NOT NULL)
  GROUP BY  wcs_user_sk
)q05_user_clicks_in_cat
INNER JOIN customer ct ON wcs_user_sk = c_customer_sk
INNER JOIN customer_demographics ON c_current_cdemo_sk = cd_demo_sk
;
