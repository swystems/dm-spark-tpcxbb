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
timeout = int(sys.argv[1])

if __name__ == "__main__":

	# requires data to be in format <user_sk>\t<timestamp>\t<item_sk>, clustered by user_sk and sorted by <timestamp> ascending
	line = ''
	current_uid = ''
	last_click_time = ''
	perUser_sessionID_counter = 1

	try:
		# algorithm expects input lines to be clustered by user_sk and sorted by <timestamp> ascending
		for line in sys.stdin:

			user_sk, tstamp_str, item_sk  = line.strip().split("\t")
			tstamp = int(tstamp_str)

			# reset if next partition beginns
			if current_uid != user_sk:
				current_uid = user_sk
				perUser_sessionID_counter = 1
				last_click_time=tstamp

			# time between clicks exceeds session timeout?
			if tstamp - last_click_time > timeout:
				perUser_sessionID_counter += 1

			last_click_time =tstamp
			print("%s\t%s_%s" % (item_sk, user_sk, str(perUser_sessionID_counter) ))

	except:
		## should only happen if input format is not correct, like 4 instead of 5 tab separated values
		logging.basicConfig(level=logging.DEBUG)
		logging.info("sys.argv[1] timeout: " +str(timeout) + " line from hive: \"" + line + "\"")
		logging.exception("Oops:")
		raise
		sys.exit(1)