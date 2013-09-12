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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ConfigurationVariable
 * @desc
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Moonie Song
 */
public class ConfigurationVariable {

	/**
	 * Used In MultiStep-Job Codes, 'def_'
	 */
	public static String MAP_OUTPUT_RECORDS_CNT = "def_mapOutputRecordCnt";
	public static String MINMAX_VALUE = "def_minMaxValue";

	public static String NUMERIC_ATTRIBUTE = "attr_numeric";
	public static String NOMINAL_ATTRIBUTE = "attr_nominal";
	
	
	private static boolean isDefinedArgumentName(String str)
	{
		if(str.equals(ArgumentsConstants.INPUT_PATH)
				|| str.equals(ArgumentsConstants.OUTPUT_PATH)
//				|| str.equals(ArgumentsConstants.REDUCER_CNT)
				|| str.equals(ArgumentsConstants.MR_JOB_STEP)
				
				|| str.equals(ArgumentsConstants.DELIMITER)
				|| str.equals(ArgumentsConstants.HELP)
				|| str.equals(ArgumentsConstants.TEMP_DELETE)
				
				|| str.equals(ArgumentsConstants.TARGET_INDEX)
				|| str.equals(ArgumentsConstants.NOMINAL_INDEX)
				|| str.equals(ArgumentsConstants.EXCEPTION_INDEX)
								
				|| str.equals(ArgumentsConstants.NORMALIZE)
				|| str.equals(ArgumentsConstants.REMAIN_FIELDS)
				
				|| str.equals(ArgumentsConstants.CERTAINTY_FACTOR_MAX)
				
				|| str.equals(ArgumentsConstants.DISTANCE_ALGORITHM)
				|| str.equals(ArgumentsConstants.MAX_ITERATION)
				|| str.equals(ArgumentsConstants.CLUSTER_COUNT)
				
				/**
				 * Add here Compare-code for Variable (User Argument Name)
				 */
				
				) return true;
		
		
		return false;
	}
	
	public static boolean setFromArguments(String[] args, Configuration conf) throws IOException 
	{
		String argName = "";
		String argValue = "";
		for (int i=0; i<args.length; ++i) 
        {
			argName = args[i];
			
			if(isDefinedArgumentName(argName))
			{
				argValue = args[++i];
				
				if (argName.equals(ArgumentsConstants.OUTPUT_PATH) && FileSystem.get(conf).exists(new Path(argValue)))
	            {
					System.err.println("Argument Error: Output Path '" + argValue + "' is aleady exist.!!");
                	return false;
	            }
				
				conf.set(argName, argValue);
			}
			else 
			{
				System.err.println("Argument Error: Unknowned Argument '" + argName + "'");
				return false;
			}
        }
        
        return true;
	}

}
