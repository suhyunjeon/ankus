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
package org.ankus;

import java.io.*;
import java.util.Random;

/**
 * GenerateDataSet
 * @desc
 *      Generate a sample data set what you want.
 *      Supported
 *          1. Boolean, 2. Numeric
 * @author Suhyun Jeon
 * @version 0.0.1
 */
public class GenerateDataSet {

    // Set separated for tab. You can change what you want
    private static String delimiter = "\t";
    private static String type = null;
    private static Integer columnSize = null;
    private static Integer rowSize = null;
    private static Integer max = null;
    private static String filePath = null;

    public static void main(String args[]){

        if(args.length < 1){
            printUsage();
            return;
        }

        initArguments(args);

        if(type.equals("boolean")){
            // Generate random boolean dataset
            String booleanDataSet = generateRandomBooleanSet(columnSize, rowSize);
            Boolean booleanFile = createFile(booleanDataSet);
            if(booleanFile){
                System.out.println("Successed! Generate "+filePath+" file.");
            }else{
                System.err.println("Failed! Generate "+filePath+" file. Try again.");
            }

        }else if(type.equals("numeric")){
            // Generate random numeric dataset
            String numericDataSet = generateRandomNumericSet(columnSize, rowSize, max);
            Boolean numericFile = createFile(numericDataSet);
            if(numericFile){
                System.out.println("Successed! Generate "+filePath+".");
            }else{
                System.err.println("Failed! Generate "+filePath+" file. Try again.");
            }
        }
    }

    public static String generateRandomBooleanSet(int columnSize, int rowSize){
        String randomData = null;

        StringBuffer stringBuffer = new StringBuffer();
        for(int i=1; i<rowSize+1; i++){
            String binary = "";

            for(int j=0; j<columnSize; j++){
                int x = 0;
                Random random = new Random();
                if(random.nextBoolean()){
                    x = 1;
                }
                binary += x + delimiter;
            }

            randomData = "id-" + i + delimiter + binary;
            stringBuffer.append(randomData.substring(0, randomData.length() - 1));
            if(i != rowSize){
                stringBuffer.append("\n");
            }
        }
        return stringBuffer.toString();
    }

    public static String generateRandomNumericSet(int columnSize, int rowSize, int max){
        String randomData = null;
        StringBuffer stringBuffer = new StringBuffer();

        for(int i=1; i<columnSize+1; i++){
            String numeric = "";

            for(int j=0; j<rowSize; j++){
                Random random = new Random();
                int x = random.nextInt(max);
                numeric += x + delimiter;
            }

            randomData = "id-" + i + delimiter + numeric;
            stringBuffer.append(randomData.substring(0, randomData.length() - 1));
            if(i != columnSize){
                stringBuffer.append("\n");
            }
        }
        return stringBuffer.toString();
    }

    public static Boolean createFile(String text){
        try {

            File file = new File(filePath);
            BufferedWriter output = new BufferedWriter(new FileWriter(file));
            output.write(text);
            output.close();
            return true;
        } catch ( IOException e ) {
            e.printStackTrace();
            return false;
        }
    }

    private static void printUsage() {
        System.out.println("============================================================");
        System.out.println("Data generation for test ");
        System.out.println("Usage: java -jar ankus-generate-0.1.jar ");
        System.out.println("           [-type < {boolean | numeric} >]");
        System.out.println("           [-columnSize <column size>]");
        System.out.println("           [-rowSize <row size>]");
        System.out.println("           [-filePath <file path location>]");
        System.out.println("If you generate boolean file, you have to skip [-max <max value>] parameter.");
        System.out.println("           [-max <max value>]");
        System.out.println("============================================================");
    }

    private static void initArguments(String[] args){
        try{
            for (int i = 0; i < args.length; ++i) {
                if ("-type".equals(args[i])) {
                    type = args[++i];
                } else if ("-columnSize".equals(args[i])) {
                    columnSize = Integer.parseInt(args[++i]);
                } else if ("-rowSize".equals(args[i])) {
                    rowSize = Integer.parseInt(args[++i]);
                } else if ("-max".equals(args[i])) {
                    max = Integer.parseInt(args[++i]);
                } else if ("-filePath".equals(args[i])){
                    filePath = args[++i];
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
