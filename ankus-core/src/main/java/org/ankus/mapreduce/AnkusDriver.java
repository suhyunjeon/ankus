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
package org.ankus.mapreduce;

import org.ankus.mapreduce.algorithms.classification.id3.ID3Driver;
import org.ankus.mapreduce.algorithms.clustering.em.EMDriver;
import org.ankus.mapreduce.algorithms.clustering.kmeans.KMeansDriver;
import org.ankus.mapreduce.algorithms.preprocessing.normalize.NormalizeDriver;
import org.ankus.mapreduce.algorithms.correlation.booleanset.BooleanSetDriver;
import org.ankus.mapreduce.algorithms.correlation.numericset.NumericSetDriver;
import org.ankus.mapreduce.algorithms.recommendation.recommender.driver.RecommendationDriver;
import org.ankus.mapreduce.algorithms.recommendation.similarity.cfbased.CFBasedSimilarityDriver;
import org.ankus.mapreduce.algorithms.recommendation.similarity.contentbased.ContentBasedSimilarityDriver;
import org.ankus.mapreduce.algorithms.correlation.stringset.StringSetDriver;
import org.ankus.mapreduce.algorithms.statistics.certaintyfactorsum.CertaintyFactorSumDriver;
import org.ankus.mapreduce.algorithms.statistics.nominalstats.NominalStatsDriver;
import org.ankus.mapreduce.algorithms.statistics.numericstats.NumericStatsDriver;
import org.ankus.mapreduce.verify.compare.CompareDriver;
import org.ankus.mapreduce.verify.recommendation.prediction.PredictionForVerifyDriver;
import org.ankus.mapreduce.verify.recommendation.rmse.RMSEDriver;
import org.ankus.util.Constants;
import org.apache.hadoop.util.ProgramDriver;

import java.lang.String;

/**
 * A description of an map/reduce program based on its class and a human-readable description.
 * @desc
 *      Collected constants of general utility
 * @version 0.0.1
 * @date : 2013.07.02
 * @update : 2013.10.7
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class AnkusDriver {

    public static void main(String[] args) {
        ProgramDriver programDriver = new ProgramDriver();
        
        try
        {
            /**
        	 * Similarity and distance algorithms
        	 */
        	// Boolean Set
        	programDriver.addClass(Constants.ALGORITHM_BOOLEAN_DATA_CORRELATION, BooleanSetDriver.class, "BooleanSet driver based map/reduce program that computes the data of the boolean set in the input files.");
        	
            // Numerical Set 
        	programDriver.addClass(Constants.ALGORITHM_NUMERIC_DATA_CORRELATION, NumericSetDriver.class, "NumericSet driver based map/reduce program that computes the data of the numeric set in the input files.");
        	
            // String Set 
        	programDriver.addClass(Constants.ALGORITHM_STRING_DATA_CORRELATION, StringSetDriver.class, "StringSet driver based map/reduce program that computes the data of the string set in the input files.");


            programDriver.addClass(Constants.ALGORITHM_COLLABORATIVE_FILTERING_BASED_SIMILARITY, CFBasedSimilarityDriver.class, "");
            /**
        	 * Recommendation system by user-based or item-based or content-based
        	 */
            programDriver.addClass(Constants.ALGORITHM_RECOMMENDATION, RecommendationDriver.class, "Recommendation driver user-based or item-based or content-based map/reduce program that computes the data of the data set in the input files.");

            /**
             * Contents-based similarity
             */
            programDriver.addClass(Constants.ALGORITHM_CONTENT_BASED_SIMILARITY, ContentBasedSimilarityDriver.class, "");

            /**
             * Verify
             */
            programDriver.addClass(Constants.ALGORITHM_PREDICTION, PredictionForVerifyDriver.class, "Prediction driver user based map/reduce program that computes the data of the data set in the input files.");

            /**
             * Verify for compare
             */
            programDriver.addClass(Constants.ALGORITHM_COMPARE, CompareDriver.class, "Compare driver user based map/reduce program that computes the data of the data set in the input files.");

            /**
             * RMSE
             */
            programDriver.addClass(Constants.ALGORITHM_RMSE, RMSEDriver.class, "RMSE driver user based map/reduce program that computes the data of the data set in the input files.");

            /**
        	 * Development Finish Classes (need to generate comments and documents of codes)>
        	 * 		statistics.NumericStatsDriver
        	 * 		statistics.NominalStatsDriver
        	 * 		statistics.CertaintyFactorSumDriver
        	 * 		preprocessing.Normalize
        	 */
        	programDriver.addClass(Constants.ALGORITHM_NUMERIC_STATS, NumericStatsDriver.class, "Statistics for Numeric Attributes of Data");
        	programDriver.addClass(Constants.ALGORITHM_NOMINAL_STATS, NominalStatsDriver.class, "Statistics(frequency/ratio) for Nominal Attributes of Data");
        	programDriver.addClass(Constants.ALGORITHM_CERTAINTYFACTOR_SUM, CertaintyFactorSumDriver.class, "Certainty Factor based Summation for Numeric Attributes of Data");
        	programDriver.addClass(Constants.ALGORITHM_NORMALIZE, NormalizeDriver.class, "Normalization for Numeric Attributes of Data");
        	programDriver.addClass(Constants.ALGORITHM_KMEANS_CLUSTERING, KMeansDriver.class, "K-means clustering Algorithm");

            programDriver.addClass(Constants.ALGORITHM_EM_CLUSTERING, EMDriver.class, "EM clustering Algorithm");
            programDriver.addClass(Constants.ALGORITHM_ID3_CLASSIFICATION, ID3Driver.class, "ID3 classification Algorithm");

        	programDriver.driver(args);
        	
        	// Success
        	System.exit(0);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}