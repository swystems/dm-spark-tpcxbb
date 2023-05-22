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


-- based on tpc-ds q90
-- What is the ratio between the number of items sold over
-- the internet in the morning (7 to 8am) to the number of items sold in the evening
-- (7 to 8pm) of customers with a specified number of dependents. Consider only
-- websites with a high amount of content.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable

set hive.exec.compress.output=false;
set hive.exec.compress.output;




-- Begin: the real query part
EXPLAIN 
SELECT CASE WHEN pmc > 0 THEN amc/pmc  ELSE -1.00 END AS am_pm_ratio
 FROM (
        SELECT SUM(amc1) AS amc, SUM(pmc1) AS pmc
        FROM(
         SELECT
         CASE WHEN t_hour BETWEEN  ${q14_morning_startHour} AND ${q14_morning_endHour} THEN COUNT(1) ELSE 0 END AS  amc1,
         CASE WHEN t_hour BETWEEN ${q14_evening_startHour} AND ${q14_evening_endHour} THEN COUNT(1) ELSE 0 END AS pmc1
          FROM web_sales ws
          JOIN household_demographics hd ON (hd.hd_demo_sk = ws.ws_ship_hdemo_sk and hd.hd_dep_count = ${q14_dependents} )
          JOIN web_page wp ON (wp.wp_web_page_sk = ws.ws_web_page_sk and wp.wp_char_count BETWEEN ${q14_content_len_min} AND ${q14_content_len_max} )
          JOIN time_dim td ON (td.t_time_sk = ws.ws_sold_time_sk and td.t_hour IN (${q14_morning_startHour},${q14_morning_endHour},${q14_evening_startHour},${q14_evening_endHour}))
          GROUP BY t_hour) cnt_am_pm
  ) sum_am_pm;


--result is a single line
;
