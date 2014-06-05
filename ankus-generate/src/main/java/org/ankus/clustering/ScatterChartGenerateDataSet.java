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
package org.ankus.clustering;

import java.io.*;
import java.util.*;

/**
 * ScatterChartGenerateDataSet
 * @desc
 *      Generate a sample data set for clustering dataset.
 * @author Suhyun Jeon
 * @date 2014.01.06
 * @version 0.1
 */
public class ScatterChartGenerateDataSet {

    public static void main(String args[]) throws FileNotFoundException {

        readFile();
        // Generate random numeric dataset
//        String numericDataSet = generateRandomNumericSet(columnSize, rowSize, max);
//        Boolean numericFile = createFile(numericDataSet);
//        if(numericFile){
//            System.out.println("Successed! Generate "+filePath+".");
//        }else{
//            System.err.println("Failed! Generate "+filePath+" file. Try again.");
//        }
    }

    public static void readFile() throws FileNotFoundException {

        String filePath = "/Users/suhyunjeon/Downloads/kmeans-result.txt";
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(filePath)));

        List<String> list1 = new ArrayList<String>();
        List<String> list2 = new ArrayList<String>();
        List<String> allList = new ArrayList<String>();

        Map<String, String> map1 = new HashMap<String, String>();
        Map<String, String> map2 = new HashMap<String, String>();


        // 여기서 X -> 0번째 index, Y -> 1번째 index로 가정한다.
        String color = null;
        String attribute1 = null;
        String attribute2 = null;
//        String attribute3 = null;
//        String attribute4 = null;

        Double[][][] array = new Double[3][][];
        while(scanner.hasNextLine()){

            String[] data = scanner.nextLine().split(",");

            attribute1 = data[0];
            attribute2 = data[1];

            color = data[4];

//            System.out.println("color1 : "+color1);

            for(int i=0; i<data.length; i++){
                for(int j=i; j<data.length; j++){

                    list1.add(attribute1);
                    list2.add(attribute2);

                    map1.put(color, attribute1);
                    map1.put(color, attribute2);



                    if(data[i].equals(data[j])){
//                        System.out.println("same data....");
                    }
                }
            }

            System.out.println("map1 : "+map1);
//            System.out.println("list2 : "+list2);
        }
    }





}
