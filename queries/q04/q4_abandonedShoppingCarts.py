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


if __name__ == "__main__":
    # lines are expected to be grouped by sessionid and presorted by timestamp
    line = ''
    current_key = ''
    session_row_counter = 0
    last_order_row = -1
    last_dynamic_row = -1

    try:

        for line in sys.stdin:

            wptype, sessionid = line.strip().split("\t")

            if current_key != sessionid:
                # is abandoned shopping carts?
                if last_dynamic_row > last_order_row:
                    # if last_dynamic_row > last_order_row and (last_order_row == -1 or last_dynamic_tstamp >= last_order_tstamp ):
                    print(session_row_counter)

                # reset for next sessionid
                session_row_counter = 1
                current_key = sessionid
                last_order_row = -1
                last_dynamic_row = -1

            else:
                session_row_counter = session_row_counter + 1

            if wptype == 'order':
                last_order_row = session_row_counter
            if wptype == 'dynamic':
                last_dynamic_row = session_row_counter

            # debug print
            #print "Debug: %s\t%s\t%s\t===\t%s\t%s" % (current_key,wptype,sessionid,last_order_row,last_dynamic_row)

        # process last tuple
        if last_dynamic_row > last_order_row:
            # if last_dynamic_row > last_order_row and (last_order_row == -1 or last_dynamic_tstamp >= last_order_tstamp ):
            print(session_row_counter)

    except:
        # should only happen if input format is not correct
        logging.basicConfig(level=logging.DEBUG)
        logging.info("line from hive: \"" + line + "\"")
        logging.exception("Oops:")
        raise
        sys.exit(1)
