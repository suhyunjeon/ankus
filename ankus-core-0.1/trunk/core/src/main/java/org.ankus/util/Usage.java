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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Usage
 * @desc
 *      Display format of commands
 * @version 0.0.1
 * @date : 2013.08.10
 * @author Suhyun Jeon
 */
public class Usage {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(Usage.class);

    public static void printUsage(String algorithm){

        String ankusVersionJarName = "ankus-core-0.0.1.jar";
        String delimiterSeparateValues = "< {tab | comma | colon} >";

        // Each algorithms description
        String description = null;
        // Each algorithms parameter
        StringBuffer parameters = new StringBuffer();

        System.out.println("al : "+algorithm);

        if(algorithm.equals(Constants.ALGORITHM_BOOLEAN_DATA_CORRELATION)){
            description = "BooleanSet driver based on map/reduce program that computes the data of the boolean set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.KEY_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ jaccard | dice | hamming }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        }else if(algorithm.equals(Constants.ALGORITHM_NUMERIC_DATA_CORRELATION)){
            description = "NumericSet driver based on map/reduce program that computes the data of the numeric set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + "<path>]\n");
            parameters.append("           [" + ArgumentsConstants.KEY_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ cosine | pearson | manhattan | uclidean }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        }else if(algorithm.equals(Constants.ALGORITHM_STRING_DATA_CORRELATION)){
            description = "StringSet driver based on map/reduce program that computes the data of the string set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + "<path>]\n");
            parameters.append("           [" + ArgumentsConstants.KEY_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ edit | hamming }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        }else if(algorithm.equals(Constants.ALGORITHM_COLLABORATIVE_FILTERING_BASED_SIMILARITY)){
            description = "Collaborative filtering CF based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + "<path>]\n");
            parameters.append("           [" + ArgumentsConstants.BASED_TYPE + " <string>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ cosine | pearson }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        }else if(algorithm.equals(Constants.ALGORITHM_USER_BASED_RECOMMENDATION)){
            description = "User-based recommendation system based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.SIMILARITY_DATA_INPUT + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        }else if(algorithm.equals(Constants.ALGORITHM_ITEM_BASED_RECOMMENDATION)){
            description = "Item-based recommendation system based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.SIMILARITY_DATA_INPUT + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        }else if(algorithm.equals(Constants.ALGORITHM_NUMERIC_STATS)){
            description = "Numeric Statistics Computation based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.MR_JOB_STEP + " <1|2>]  default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        }else if(algorithm.equals(Constants.ALGORITHM_NOMINAL_STATS)){
            description = "Nominal Statistics(frequency/ratio) Computation based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        }else if(algorithm.equals(Constants.ALGORITHM_CERTAINTYFACTOR_SUM)){
            description = "Certainty Factor based Summation based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.CERTAINTY_FACTOR_MAX + " <max_value>] default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.MR_JOB_STEP + " <1|2>]  default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        }else if(algorithm.equals(Constants.ALGORITHM_NORMALIZE)){
            description = "Numeric Data Normalization based on map/reduce program that converts the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.REMAIN_FIELDS + " <true|false>] default value: true\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.MR_JOB_STEP + " <1|2>]  default value: 1(for numeric stats)\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        }else if(algorithm.equals(Constants.ALGORITHM_KMEANS_CLUSTERING)){
            description = "K-Means Clustering based on map/reduce program that uses the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("           [" + ArgumentsConstants.NOMINAL_INDEX + " <index_list>]\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.NORMALIZE + " <true|false>] default value: true\n");
            parameters.append("           [" + ArgumentsConstants.MAX_ITERATION + " <count>] default value: 1(do not recommend)\n");
            parameters.append("           [" + ArgumentsConstants.CLUSTER_COUNT + " <count>] default value: 1(do not recommend)\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");
        }

        logger.info("=========================================================================================================");
        logger.info(description);
        logger.info("---------------------------------");
        logger.info(parameters.toString());
        logger.info("=========================================================================================================");

    }
}
