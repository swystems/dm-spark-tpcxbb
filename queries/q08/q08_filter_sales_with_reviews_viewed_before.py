#
# Copyright (C) 2020 Transaction Processing Performance Council (TPC) and/or its contributors.
# This file is part of a software package distributed by the TPC
# The contents of this file have been developed by the TPC, and/or have been licensed to the TPC under one or more contributor
# license agreements.
# This file is subject to the terms and conditions outlined in the End-User
# License Agreement (EULA) which can be found in this distribution (EULA.txt) and is available at the following URL:
# http://www.tpc.org/TPC_Documents_Current_Versions/txt/EULA.txt
# Unless required by applicable law or agreed to in writing, this software is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, and the user bears the entire risk as to quality
# and performance as well as the entire cost of service or repair in case of defect. See the EULA for more details.
# 
#


#
# Copyright 2015-2020 Intel Corporation.
# This software and the related documents are Intel copyrighted materials, and your use of them 
# is governed by the express license under which they were provided to you ("License"). Unless the 
# License provides otherwise, you may not use, modify, copy, publish, distribute, disclose or 
# transmit this software or the related documents without Intel's prior written permission.
# 
# This software and the related documents are provided as is, with no express or implied warranties, 
# other than those that are expressly stated in the License.
# 
#


import sys
import logging
import traceback
import os
import time
from time import strftime

web_page_type_filter=sys.argv[1] 
seconds_before_sale_filter = int(sys.argv[2])


if __name__ == "__main__":
	line = ''
	try:
		current_key = ''
		last_review_date=-1
		#sales_sk should be distinct
		last_sales_sk = ''
		#expects input to be partitioned by uid and sorted by date_sk (and timestamp) ascending


		for line in sys.stdin:
			# lustered by wcs_user_sk and by wcs_user_sk, tstamp_inSec_str, wcs_sales_sk, wp_type ascending in this order => ensured by hive
			wcs_user_sk, tstamp_inSec_str, wcs_sales_sk, wp_type = line.strip().split("\t")

			#reset on partition change
			if current_key != wcs_user_sk :
				current_key = wcs_user_sk
				last_review_date = -1
				last_sales_sk = ''

			tstamp_inSec = int(tstamp_inSec_str)

			#found review before purchase, save last review date
			if wp_type == web_page_type_filter:
				last_review_date = tstamp_inSec
				continue

			#if we encounter a sold item ( wcs_sales_sk.isdigit() => valid non null value) and a user looked at a review within 'seconds_before_sale_filter' => print found sales_sk backt to hive
			#if last_review_date > 0  and (tstamp_inSec - last_review_date) <= seconds_before_sale_filter and wcs_sales_sk.isdigit()  :   #version with duplicate sales_sk's
			if last_review_date > 0  and (tstamp_inSec - last_review_date) <= seconds_before_sale_filter and wcs_sales_sk.isdigit() and last_sales_sk != wcs_sales_sk : #version reduced duplicate sales_sk's
				last_sales_sk = wcs_sales_sk
				print (wcs_sales_sk)

	except:
		## should only happen if input format is not correct, like 4 instead of 5 tab separated values
		logging.basicConfig(level=logging.DEBUG)
		logging.info('web_page_type_filter: ' + web_page_type_filter )
		logging.info('seconds_before_sale_filter: ' + seconds_before_sale_filter )
		logging.info("line from hive: \"" + line + "\"")
		logging.exception("Oops:")
		raise
		sys.exit(1)
