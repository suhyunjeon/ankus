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
package org.ankus.mapreduce.algorithms.recommendation.similarity.cfbased;

import java.io.IOException;

import org.ankus.io.TextDoubleTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CalculationCFBasedSimilarityReducer
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class CalculationCFBasedSimilarityReducer extends Reducer<TextTwoWritableComparable, TextDoubleTwoPairsWritableComparable, TextTwoWritableComparable, DoubleWritable> {
    
	private String algorithmOption;
    private String threshold;
    private String commonCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.algorithmOption = configuration.get(Constants.ALGORITHM_OPTION);
        this.threshold = configuration.get(Constants.THRESHOLD);
        this.commonCount = configuration.get(Constants.COMMON_COUNT);
    }

    @Override
    protected void reduce(TextTwoWritableComparable key, Iterable<TextDoubleTwoPairsWritableComparable> values, Context context) throws IOException, InterruptedException {

        if(algorithmOption.equals(Constants.COSINE_COEFFICIENT)){
            int docProduct = 0;
            int normItemID1 = 0;
            int normItemID2 = 0;
            // Count values for sigma(standard deviation)
            int n = 0;

            for(TextDoubleTwoPairsWritableComparable textDoublePairsWritableComparable : values) {
                n++;
                docProduct += textDoublePairsWritableComparable.getNumber1() * textDoublePairsWritableComparable.getNumber2();

                normItemID1 += Math.pow(textDoublePairsWritableComparable.getNumber1(), 2);
                normItemID2 += Math.pow(textDoublePairsWritableComparable.getNumber2(), 2);
            }

            if(n >= Integer.parseInt(commonCount)){
                double cosineCoefficient = docProduct / (Math.sqrt(normItemID1) * Math.sqrt(normItemID2));
                if(cosineCoefficient >= Double.parseDouble(threshold)){
                    context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", cosineCoefficient))));
                }
            }

        }else if(algorithmOption.equals(Constants.PEARSON_COEFFICIENT)){
        	double sumUserId1 = 0.0d;
        	double sumUserId2 = 0.0d;
        	double squareSumUserId1 = 0.0d;
        	double squareSumUserId2 = 0.0d;
        	double totalSumUserIds = 0.0d;

        	// PCC(Pearson Correlation Coefficient) variable 
        	double r = 0.0d;
        	int n = 0;
        	
        	for(TextDoubleTwoPairsWritableComparable textDoublePairsWritableComparable : values) {
        		
        		// Count values for sigma(standard deviation)
            	n++;
            	
        		//  Sum of item values for users
        		sumUserId1 += textDoublePairsWritableComparable.getNumber1();
        		sumUserId2 += textDoublePairsWritableComparable.getNumber2();
        		
        		// Sum of squares for users
        		squareSumUserId1 += Math.pow(textDoublePairsWritableComparable.getNumber1(), 2);
        		squareSumUserId2 += Math.pow(textDoublePairsWritableComparable.getNumber2(), 2);
        		
        		// Calculate sum of times for users
        		totalSumUserIds += (textDoublePairsWritableComparable.getNumber1() * textDoublePairsWritableComparable.getNumber2()); 
        	}

            if(n >= Integer.parseInt(commonCount)){
                // 1. Calculate numerator
                double numerator = totalSumUserIds - ((sumUserId1 * sumUserId2) / n);

                // 2. Calculate each of the denominator user1 and denominator user2
                double denominatorUserId1 = squareSumUserId1 - ((Math.pow(sumUserId1, 2)) / n);
                double denominatorUserId2 = squareSumUserId2 - ((Math.pow(sumUserId2, 2)) / n);

                // 3. Calculate denominator
                double denominator = Math.sqrt(denominatorUserId1 * denominatorUserId2);

                // 4. Calculate PCC(Pearson Correlation Coefficient)
                if(denominator == 0) {
                    r = 0.0d;
                }else{
                    r = numerator / denominator;
                }

                if(r >= Double.parseDouble(threshold)){
                    context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", r))));
                }
            }
        }
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
