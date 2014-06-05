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

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.ankus.util.HadoopUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

/**
 * RMSEDriver
 * @desc
 *      Calculating RMSE(base) by ankus recommendation system.
 *      This is rearrange recommendation system. There is no candidate step.
 *      Only support User-based Collaborative Filtering recommendation algorithms
 *      Runs a recommendation job as a series of map/reduce
 * @version 0.1
 * @date : 2013.11.18
 * @author Suhyun Jeon
 */
public class RMSEDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

    private String input = null;
    private String output = null;
    private String delimiter = null;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new RMSEDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        initArguments(args);

        Job job = new Job(this.getConf());
        job = HadoopUtil.prepareJob(job, new Path(input), new Path(output), RMSEDriver.class,
                RMSEMapper.class, NullWritable.class, DoubleWritable.class,
                RMSEReducer.class, NullWritable.class, DoubleWritable.class);

        job.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step1 = job.waitForCompletion(true);
        if (!(step1)) return -1;

        return 0;
    }

    private void initArguments(String[] args) {
        try {
            for (int i = 0; i < args.length; ++i) {
                if (ArgumentsConstants.INPUT_PATH.equals(args[i])) {
                    input = args[++i];
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