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


TEMP_RESULT_TABLE="${TABLE_PREFIX}_temp_result"
TEMP_RESULT_DIR="$TEMP_DIR/$TEMP_RESULT_TABLE"

BINARY_PARAMS+=(--hiveconf TEMP_RESULT_TABLE=$TEMP_RESULT_TABLE --hiveconf TEMP_RESULT_DIR=$TEMP_RESULT_DIR)

HDFS_RESULT_FILE="${RESULT_DIR}/cluster.txt"

query_run_main_method () {
 
 QUERY_SCRIPT="$QUERY_DIR/$QUERY_NAME.sql"

 if [ ! -r "$QUERY_SCRIPT" ]
  then
    echo "SQL file $QUERY_SCRIPT can not be read."
    exit 1
  fi

  #EXECUTION Plan:
  #step 1.  hive q25.sql    :  Run hive querys to extract kmeans input data
  #step 2.  spark kmeans    :  Calculating k-means"
  #step 3.  hive && hdfs     :  cleanup.sql && hadoop fs rm MH


  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 1 ]] ; then
    echo "========================="
    echo "$QUERY_NAME Step 1/5: Executing spark sql queries"
    echo "tmp output: ${TEMP_RESULT_DIR}"
    echo "========================="
    # Write input for k-means into temp table
    runCmdWithErrorCheck runEngineCmd --name "$QUERY_NAME" -f "$QUERY_SCRIPT"
    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi

  fi


  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 2 ]] ; then
    ##########################
    #run with spark
    ##########################
    input="--fromHiveMetastore true --input ${BIG_BENCH_DATABASE}.${TEMP_RESULT_TABLE}"
    output="${RESULT_DIR}/"
    cluster_centers=8
    clustering_iterations=20
    initialClusters="" #empty: random initial cluster (fixed seed)

    if [[ "$BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK" == "spark" ]] ; then
      main_class="io.bigdatabenchmark.v2.queries.KMeansClustering"
      job_jar="$BIG_BENCH_QUERY_RESOURCES/bigbench-ml-spark-2x.jar"
    elif [[ -z "$BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK" || "$BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK" == "spark-2.3" ]]; then
      main_class="io.bigdatabenchmark.v2.queries.KMeansClustering"
      job_jar="$BIG_BENCH_QUERY_RESOURCES/bigbench-ml-spark-2.3.jar"
    else
      echo "BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK parameter has no matching implmentation or was empty: $BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK  "
      return 1
    fi

    cmd="${BIG_BENCH_ENGINE_SPARK_SQL_ML_FRAMEWORK_SPARK_BINARY} --class ${main_class} ${job_jar} ${input} --output ${output} --num-clusters ${cluster_centers} --iterations ${clustering_iterations} --query-num ${QUERY_NAME} ${initialClusters} --saveClassificationResult true --saveMetaInfo true --verbose false"

    echo "========================="
    echo "$QUERY_NAME Step 2/3: Calculating KMeans with spark"
    echo "intput: ${input}"
    echo "result output: $output"
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
    
    runCmdWithErrorCheck hadoop fs -rm -r -f "$TEMP_RESULT_DIR"
    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi
}

query_run_clean_method () {
  runCmdWithErrorCheck runEngineCmd -e "DROP TABLE IF EXISTS $TEMP_TABLE; DROP TABLE IF EXISTS $TEMP_RESULT_TABLE; DROP TABLE IF EXISTS $RESULT_TABLE;"
  runCmdWithErrorCheck hadoop fs -rm -r -f "$HDFS_RESULT_FILE"
  return $?
}

query_run_validate_method () {
  # perform exact result validation if using SF 1, else perform general sanity check
#######
# Do not perform exact result validation for q25 since Spark's KMeans implementation produces different results based on
# the partitioning of the input data
#
# See https://issues.apache.org/jira/browse/SPARK-21679
#######
#  if [ "$BIG_BENCH_SCALE_FACTOR" -eq 1 ]
#  then
#    local VALIDATION_PASSED="1"
#
#    if [ ! -f "$VALIDATION_RESULTS_FILENAME" ]
#    then
#      echo "Golden result set file $VALIDATION_RESULTS_FILENAME not found"
#      VALIDATION_PASSED="0"
#    fi
#
#    if diff -q "$VALIDATION_RESULTS_FILENAME" <(hadoop fs -cat "$RESULT_DIR/*")
#    then
#      echo "Validation of $VALIDATION_RESULTS_FILENAME passed: Query returned correct results"
#    else
#      echo "Validation of $VALIDATION_RESULTS_FILENAME failed: Query returned incorrect results"
#      VALIDATION_PASSED="0"
#    fi
#    if [ "$VALIDATION_PASSED" -eq 1 ]
#    then
#      echo "Validation passed: Query results are OK"
#    else
#      echo "Validation failed: Query results are not OK"
#      return 1
#    fi
#  else
    if [ `hadoop fs -cat "$RESULT_DIR/*" | head -n 10 | wc -l` -ge 1 ]
    then
      echo "Validation passed: Query returned results"
    else
      echo "Validation failed: Query did not return results"
      return 1
    fi
#  fi
}
