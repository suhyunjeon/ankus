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
package org.ankus.mapreduce.verify.compare;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.ankus.util.HadoopUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompareDriver
 *
 * @author Suhyun Jeon
 * @version 0.1
 * @desc User-based or item-based Collaborative Filtering recommendation algorithms
 * Runs a recommendation job as a series of map/reduce
 * @date : 2013.10.01
 */
public class CompareDriver extends Configured implements Tool {

    private String input = null;
    private String recommendedDataInput = null;
    private String output = null;
    private String delimiter = null;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CompareDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        initArguments(args);

        Job job = new Job(this.getConf());
        job = HadoopUtil.prepareJob(job, new Path(input), new Path(recommendedDataInput), new Path(output), CompareDriver.class,
                RecommendationResultMapper.class, OriginalDataMapper.class, Text.class, Text.class,
                CompareReducer.class, Text.class, Text.class);

        job.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step = job.waitForCompletion(true);
        if (!(step)) return -1;

        return 0;
    }

    private void initArguments(String[] args) {
        try {
            for (int i = 0; i < args.length; ++i) {
                if (ArgumentsConstants.INPUT_PATH.equals(args[i])) {
                    input = args[++i];
                } else if (ArgumentsConstants.RECOMMENDED_DATA_INPUT.equals(args[i])) {
                    recommendedDataInput = args[++i];
                } else if (ArgumentsConstants.OUTPUT_PATH.equals(args[i])) {
                    output = args[++i];
                } else if (ArgumentsConstants.DELIMITER.equals(args[i])) {
                    delimiter = args[++i];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}