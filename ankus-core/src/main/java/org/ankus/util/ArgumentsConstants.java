/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ankus.util;

/**
 * ArgumentsConstants
 * @desc
 *      Collected constants of parameter by user
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class ArgumentsConstants {

    public static final String INPUT_PATH = "-input";
    public static final String SIMILARITY_DATA_INPUT = "-similarDataInput";
    public static final String RECOMMENDED_DATA_INPUT = "-recommendedDataInput";
    public static final String OUTPUT_PATH = "-output";
    public static final String KEY_INDEX = "-keyIndex";
    public static final String COMPUTE_INDEX = "-computeIndex";
    public static final String ALGORITHM_OPTION = "-algorithmOption";
    public static final String BASED_TYPE = "-basedType";
    public static final String THRESHOLD = "-threshold";
    public static final String MR_JOB_STEP = "-mrJobStep";
    public static final String HELP = "-help";
    public static final String TEMP_DELETE = "-tempDelete";
    public static final String COMMON_COUNT = "-commonCount";

    // common (-1:all / 0,2,5)
    public static final String TARGET_INDEX = "-indexList";
    // for some jobs (-1: none / 2,5)
    public static final String NOMINAL_INDEX = "-nominalIndexList";
    public static final String NUMERIC_INDEX = "-numericIndexList";
    // for some jobs (-1: none / 3,7)
    public static final String EXCEPTION_INDEX = "-exceptionIndexList";

    // for some jobs (true/false)
    public static final String NORMALIZE = "-normalize";
    // for some jobs (true/false)
    public static final String REMAIN_FIELDS = "-remainAllFields";

    // for certainty factor sum
    public static final String CERTAINTY_FACTOR_MAX = "-cfsumMax";

    public static final String DISTANCE_ALGORITHM = "-diatanceAlgorithm";

    // for k-means, em
    public static final String MAX_ITERATION = "-maxIteration";
    public static final String CLUSTER_COUNT = "-clusterCnt";

    public static final String CLUSTER_PATH = "def_clusterPath";

    public static final String DELIMITER = "-delimiter";
    public static final String SUB_DELIMITER = "-subDelimiter";

    // for decision tree
    public static final String RULE_PATH = "-ruleFilePath";
    public static final String CLASS_INDEX = "-classIndex";
    public static final String CLASS_CNT = "-classCnt";

    public static final String MIN_LEAF_DATA = "-minLeafData";
    public static final String PURITY = "-purity";

    // must update "ConfigurationVariable" class


    public static final String FINAL_RESULT_GENERATION = "-finalResultGen";


}
