#!/usr/bin/env bash

#
# Copyright (C) 2019 Transaction Processing Performance Council (TPC) and/or its contributors.
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
# Copyright 2015-2019 Intel Corporation.
# This software and the related documents are Intel copyrighted materials, and your use of them 
# is governed by the express license under which they were provided to you ("License"). Unless the 
# License provides otherwise, you may not use, modify, copy, publish, distribute, disclose or 
# transmit this software or the related documents without Intel's prior written permission.
# 
# This software and the related documents are provided as is, with no express or implied warranties, 
# other than those that are expressly stated in the License.
# 
#


TEMP_TABLE1="${TEMP_TABLE}_training"
TEMP_DIR1="$TEMP_DIR/$TEMP_TABLE1"
TEMP_TABLE2="${TEMP_TABLE}_testing"
TEMP_DIR2="$TEMP_DIR/$TEMP_TABLE2"

BINARY_PARAMS+=(--hiveconf TEMP_TABLE1=$TEMP_TABLE1 --hiveconf TEMP_DIR1=$TEMP_DIR1 --hiveconf TEMP_TABLE2=$TEMP_TABLE2 --hiveconf TEMP_DIR2=$TEMP_DIR2)

HDFS_RESULT_FILE="${RESULT_DIR}/classifierResult.txt"

query_run_main_method () {
 
  QUERY_SCRIPT="$QUERY_DIR/$QUERY_NAME.sql"

  if [ ! -r "$QUERY_SCRIPT" ]
  then
    echo "SQL file $QUERY_SCRIPT can not be read."
    exit 1
  fi

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 1 ]] ; then
    echo "========================="
    echo "$QUERY_NAME step 1/3: Executing spark sql queries"
    echo "tmp result1 (training data) in:" ${TEMP_DIR1}
    echo "tmp result2 (test data)     in:" ${TEMP_DIR2}
    echo "========================="

    # Write input for k-means into temp tables
    runCmdWithErrorCheck runEngineCmd --name "$QUERY_NAME" -f "$QUERY_SCRIPT"
    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 2 ]] ; then
    ##########################
    #run with spark (default) 
    ##########################
    inputTableTraining="${BIG_BENCH_DATABASE}.${TEMP_TABLE1}"
    inputTableTesting="${BIG_BENCH_DATABASE}.${TEMP_TABLE2}"
    input="--fromHiveMetastore true --inputTraining ${inputTableTraining} --inputTesting ${inputTableTesting} --lambda 0"
    output="${RESULT_DIR}/"

    if [[ "$BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK" == "spark" ]] ; then
      main_class="io.bigdatabenchmark.v2.queries.q28.NaiveBayesClassifier"
      job_jar="${BIG_BENCH_QUERY_RESOURCES}/bigbench-ml-spark-2x.jar"
    elif [[ -z "$BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK" || "$BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK" == "spark-2.3" ]]; then
      main_class="io.bigdatabenchmark.v2.queries.q28.NaiveBayesClassifier"
      job_jar="$BIG_BENCH_QUERY_RESOURCES/bigbench-ml-spark-2.3.jar"
    else
      echo "BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK parameter has no matching implmentation or was empty: $BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK  "
      return 1
    fi

    cmd="${BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK_SPARK_BINARY} --class ${main_class} ${job_jar} ${input} --output ${output} --saveClassificationResult true --saveMetaInfo true --verbose false"

    echo "========================="
    echo "$QUERY_NAME step 2/3: Train and Test Naive Bayes Classifier with spark - read from metastore table"
    echo "training and testing data:" ${inputTableTraining}
    echo "test data    :" ${TEMP_DIR2}
    echo "OUT: $RESULT_DIR"
    echo "========================="

                    echo ${cmd}
    runCmdWithErrorCheck ${cmd}

    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 3 ]] ; then
    echo "========================="
    echo "$QUERY_NAME Step 3/3: Clean up"
    echo "========================="
    runCmdWithErrorCheck runEngineCmd --name "${QUERY_NAME}_cleanup" -f "${QUERY_DIR}/cleanup.sql"
    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
    runCmdWithErrorCheck hadoop fs -rm -r -f "$TEMP_DIR"
    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi

  #echo "========================="
  #echo "to display : hadoop fs -cat $HDFS_RESULT_FILE"
  #echo "to display raw : hadoop fs -cat $HDFS_RAW_RESULT_FILE"
  #echo "========================="
}

query_run_clean_method () {
  runCmdWithErrorCheck runEngineCmd -e "DROP TABLE IF EXISTS $TEMP_TABLE1; DROP TABLE IF EXISTS $TEMP_TABLE2; DROP TABLE IF EXISTS $RESULT_TABLE;"
  runCmdWithErrorCheck hadoop fs -rm -r -f "$HDFS_RESULT_FILE"
  #runCmdWithErrorCheck hadoop fs -rm -r -f "$HDFS_RAW_RESULT_FILE"
  return $?
}

query_run_validate_method () {
  # perform exact result validation if using SF 1, else perform general sanity check
  if [ "$BIG_BENCH_SCALE_FACTOR" -eq 1 ]
  then
    local VALIDATION_PASSED="1"

    if [ ! -f "$VALIDATION_RESULTS_FILENAME" ]
    then
      echo "Golden result set file $VALIDATION_RESULTS_FILENAME not found"
      VALIDATION_PASSED="0"
    fi

    if diff -q "$VALIDATION_RESULTS_FILENAME" <(hadoop fs -cat "$RESULT_DIR/*")
    then
      echo "Validation of $VALIDATION_RESULTS_FILENAME passed: Query returned correct results"
    else
      echo "Validation of $VALIDATION_RESULTS_FILENAME failed: Query returned incorrect results"
      VALIDATION_PASSED="0"
    fi
    if [ "$VALIDATION_PASSED" -eq 1 ]
    then
      echo "Validation passed: Query results are OK"
    else
      echo "Validation failed: Query results are not OK"
      return 1
    fi
  else
    if [ `hadoop fs -cat "$RESULT_DIR/*" | head -n 10 | wc -l` -ge 1 ]
    then
      echo "Validation passed: Query returned results"
    else
      echo "Validation failed: Query did not return results"
      return 1
    fi
  fi
}

