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
 * Constants
 * @desc
 *      Collected constants of general utility
 * @version 0.0.1
 * @date : 2013.07.15
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class Constants {

   /**
    * Algorithm name for boolean sets (or drive name)
    */
    public static final String HAMMING_DISTACNE_FOR_BOOLEAN = "hamming";
    public static final String DICE_COEFFICIENT = "dice";
    public static final String JACCARD_COEFFICIENT = "jaccard";
    
    /**
     * Algorithm name for numeric sets (or driver name)
     */
    public static final String COSINE_COEFFICIENT = "cosine";
    public static final String PEARSON_COEFFICIENT = "pearson";
    public static final String TANIMOTO_COEFFICIENT = "tanimoto";
    public static final String MANHATTAN_DISTANCE = "manhattan";
    public static final String UCLIDEAN_DISTANCE = "uclidean";

    /**
     * Algorithm name for string sets (or drive name)
     */
    public static final String EDIT_DISTANCE = "edit";
    public static final String HAMMING_DISTANCE_FOR_STRING = "hamming";


    /**
     * Algorithm name for data sets (or driver name)
     */
    public static final String ALGORITHM_NUMERIC_STATS = "NumericStatistics";
    public static final String ALGORITHM_NOMINAL_STATS = "NominalStatistics";
    public static final String ALGORITHM_CERTAINTYFACTOR_SUM = "CertaintyFactorSUM";
    public static final String ALGORITHM_NORMALIZE = "Normalization";
    public static final String ALGORITHM_KMEANS_CLUSTERING = "KMeans";
    public static final String ALGORITHM_EM_CLUSTERING = "EM";
    public static final String ALGORITHM_ID3_CLASSIFICATION = "ID3";

   /**
    * Boolean type of data set by similarity and distance algorithm
    */
    public static final String ALGORITHM_BOOLEAN_DATA_CORRELATION = "BooleanDataCorrelation";

    /**
     * Numeric type of data set by similarity and distance algorithm
     */
    public static final String ALGORITHM_NUMERIC_DATA_CORRELATION = "NumericDataCorrelation";

    /**
     * String type of data set by similarity and distance algorithm
     */
    public static final String ALGORITHM_STRING_DATA_CORRELATION = "StringDataCorrelation";

    /**
     * Collaborative filtering by CF-based
     */
    public static final String ALGORITHM_COLLABORATIVE_FILTERING_BASED_SIMILARITY = "CFBasedSimilarity";

    /**
     * Collaborative filtering recommendation by user-based or item-based
     */
    public static final String ALGORITHM_USER_BASED_RECOMMENDATION = "UserBasedRecommendation";

    /**
     * Collaborative filtering recommendation by item-based or item-based
     */
    public static final String ALGORITHM_ITEM_BASED_RECOMMENDATION = "ItemBasedRecommendation";

    /**
     * Algorithm collaborative filtering option (user-based, item-based)
     */
    public static final String USER_BASED = "user";
    public static final String ITEM_BASED = "item";

    /**
     * Algorithm collaborative filtering recommender option
     */
    public static final String SIMILARITY = "similarity";
    public static final String SIMILARITY1 = "similarity1";
    public static final String SIMILARITY2 = "similarity2";
    public static final String MOVIELENS = "movielens";
    public static final String PREDICTION = "prediction";

    /**
     * Option parameters for MapReduce driver 
     */
    public static final String KEY_INDEX = "keyIndex";
    public static final String DELIMITER = "delimiter";
    public static final String ALGORITHM_OPTION = "algorithmOption";
    public static final String COMPUTE_INDEX = "computeIndex";
    public static final String THRESHOLD = "threshold";
    public static final String REMOVE_INDEX = "removeIndex";
    public static final String BASED_TYPE = "basedType";
    public static final String COMMON_COUNT = "commonCount";

    /**
     * Option parameters for create prediction dataset
     */
    public static final String ITEM_LIST_HDFS_PATH = "itemListPath";

    /**
     * Remove mode for midterm process
     */
    public static final String REMOVE_ON = "on";
    public static final String REMOVE_OFF = "off";

    /**
     * key name from config.properties
     */
    public static final String MIDTERM_PROCESS_OUTPUT_DIR = "midterm.process.output.dir";
    public static final String MIDTERM_PROCESS_OUTPUT_REMOVE_MODE = "midterm.process.output.remove.mode";

    /**
     * Encoding
     */
    public static final String UTF8 = "UTF-8";
    public static final String EUCKR = "EUC-KR";

    /**
     * Date format
     */
    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
}
