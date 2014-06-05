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
package org.ankus.mapreduce.verify.recommendation.rmse;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * RMSEReducer
 * @desc
 *      Calculating RMSE(base) by ankus recommendation system.
 *      This is rearrange recommendation system. There is no candidate step.
 *      Only support User-based Collaborative Filtering recommendation algorithms
 *      Runs a recommendation job as a series of map/reduce
 * @version 0.1
 * @date : 2013.11.18
 * @author Suhyun Jeon
 */
public class RMSEReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sumSquareValues = 0.0d;
        double rmse = 0.0d;
        double n = 0.0d;

        for(DoubleWritable doubleWritableValues : values) {
            sumSquareValues += Math.pow(doubleWritableValues.get(), 2);
            n++;
        }

        rmse = Math.sqrt(sumSquareValues / n);
        context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.2f%n", rmse))));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
