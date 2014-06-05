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

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * AnkusUtils
 * @desc
 *      Get temp directory for middle job output path
 * @version 0.0.1
 * @date : 2013.08.22
 * @author Suhyun Jeon
 */
public class AnkusUtils {

    public static Properties getConfigProperties() throws IOException, NullPointerException {

        InputStream resourceAsStream = null;
        Properties properties = new Properties();
        try{
            // Get configuration properties for hdfs output path from user
            resourceAsStream = AnkusUtils.class.getResourceAsStream("/config.properties");
            properties.load(resourceAsStream);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            resourceAsStream.close();
        }
        return properties;
    }

    public static String createDirectoryForHDFS(String outputPath) throws IOException {

        Properties configProperties = getConfigProperties();

        // Generate date format for create file name
        DateFormat dateFormat = new SimpleDateFormat(Constants.YYYYMMDDHHMMSS);
        Date date = new Date();
        String currentDate = dateFormat.format(date);

        // Get key (hdfs.output.dir) from config.properties
        String hdfsTempDir = configProperties.get(Constants.MIDTERM_PROCESS_OUTPUT_DIR).toString();

        /**
         * Check key of properties
         * If value is null, add '_prepare' of output path by user
         * Else value is not null, set value to config.properties by user
         */
        String tempDirectory = null;

        if(hdfsTempDir.equals("")){
            tempDirectory = outputPath + "_prepare";
        }else{
            tempDirectory = hdfsTempDir + "/" + currentDate;
        }

        return tempDirectory;
    }
}