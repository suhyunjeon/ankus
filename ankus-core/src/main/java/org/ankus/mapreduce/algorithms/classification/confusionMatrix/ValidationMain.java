package org.ankus.mapreduce.algorithms.classification.confusionMatrix;

import org.ankus.util.Constants;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: moonie
 * Date: 14. 5. 20
 * Time: 오후 1:15
 * To change this template use File | Settings | File Templates.
 */
public class ValidationMain {


    /**
     *
     * @param fs            target file system (for hadoop)
     * @param inputPath     confusion matrix row data (org-class, pred-class, frequency)
     * @param delimiter     input file delimiter
     * @param outputPath    final classification result (validation performance)
     * @throws Exception
     */
    public void validationGeneration(FileSystem fs, String inputPath, String delimiter, String outputPath) throws Exception
    {
        /**
         * 2. PostProcessing
         *      Reduce Result Read and Class Label Identification
         *      Make Result
         *          >> summary > total instances, correctly classified instances, incorrect ~
         *          >> confusion Matrix
         *          class, total(weighted) > TP rate, FP rate, Precision, Recall, F1-Measure
         **/

        ArrayList<String[]> readStrList = new ArrayList<String[]>();
        ArrayList<String> uniqClassList = new ArrayList<String>();
        int totalDataCnt = 0;
        int correctDataCnt = 0;
        int inCorrectDataCnt = 0;

        // multi input file check
        // in-memory load all-cross-count and uniq class list
        FileStatus[] status = fs.listStatus(new Path(inputPath));
        for(int i=0; i<status.length; i++)
        {
            FSDataInputStream fin = fs.open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

            String readStr, tokens[];
            int value;
            while((readStr=br.readLine())!=null)
            {
                tokens = readStr.split(delimiter);
                readStrList.add(tokens);

                if(!uniqClassList.contains(tokens[0])) uniqClassList.add(tokens[0]);

                value = Integer.parseInt(tokens[2]);
                totalDataCnt += value;

                if(tokens[0].equals(tokens[1])) correctDataCnt += value;
                else inCorrectDataCnt += value;
            }

            br.close();
            fin.close();
        }

        // confusion matrix
        int classCnt = uniqClassList.size();
        int confusionMatrix[][] = new int[classCnt+1][classCnt+1];
        for(String[] infoStr: readStrList)
        {
            int row = uniqClassList.indexOf(infoStr[0]);
            int col = uniqClassList.indexOf(infoStr[1]);
            int value = Integer.parseInt(infoStr[2]);
            confusionMatrix[row][col] = value;

            confusionMatrix[row][classCnt] += value;
            confusionMatrix[classCnt][col] += value;
        }

        // performance
        double tpRate[] = new double[classCnt + 1];
        double fpRate[] = new double[classCnt + 1];
        double precision[] = new double[classCnt + 1];
        double recall[] = new double[classCnt + 1];
        double f1measure[] = new double[classCnt + 1];

        for(int i=0; i<classCnt; i++)
        {
            if(confusionMatrix[i][i]==0)
            {
                tpRate[i] = 0;
                precision[i] = 0;
                recall[i] = 0;
                f1measure[i] = 0;
            }
            else
            {
                tpRate[i] = (double)confusionMatrix[i][i] / (double)confusionMatrix[i][classCnt];
                if(confusionMatrix[classCnt][i] == confusionMatrix[i][i]) fpRate[i] = 0;
                else
                {
                    fpRate[i] = ((double)confusionMatrix[classCnt][i] - (double)confusionMatrix[i][i])
                            / ((double)totalDataCnt - (double)confusionMatrix[i][classCnt]);
                }

                precision[i] = (double)confusionMatrix[i][i] / (double)confusionMatrix[classCnt][i];
                recall[i] = tpRate[i];
                f1measure[i] = 2 * (precision[i] * recall[i]) / (precision[i] + recall[i]);
            }
        }

        tpRate[classCnt] = 0;
        fpRate[classCnt] = 0;
        precision[classCnt] = 0;
        recall[classCnt] = 0;
        f1measure[classCnt] = 0;
        for(int i=0; i<classCnt; i++)
        {
            double divValue = (double)confusionMatrix[i][classCnt] / (double)totalDataCnt;
            tpRate[classCnt] += tpRate[i] * divValue;
            fpRate[classCnt] += fpRate[i] * divValue;
            precision[classCnt] += precision[i] * divValue;
            recall[classCnt] += recall[i] * divValue;
            f1measure[classCnt] += f1measure[i] * divValue;
        }


        // output file generation
        FSDataOutputStream fout = fs.create(new Path(outputPath), true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));

        bw.write("# Total Summary" + "\n");
        bw.write("Total Instances: " + totalDataCnt + "\n");
        double tmpVal = (double)correctDataCnt / (double)totalDataCnt * 100;
        String formatStr = String.format("%2.2f",tmpVal);
        bw.write("Correctly Classified Instances: " + correctDataCnt + "(" + formatStr + "%)" +"\n");
        tmpVal = (double)inCorrectDataCnt / (double)totalDataCnt * 100;
        formatStr = String.format("%2.2f",tmpVal);
        bw.write("Incorrectly Classified Instances: " + inCorrectDataCnt + "(" + formatStr + "%)" + "\n");
        bw.write("\n");

        bw.write("# Confusion Matrix" + "\n");
        bw.write("(Classified as)");
        for(String classStr: uniqClassList) bw.write("\t" + classStr);
        bw.write("\t|\ttotal\t" + "\n");
        for(int i=0; i<classCnt; i++)
        {
            bw.write(uniqClassList.get(i));
            for(int j=0; j<classCnt; j++) bw.write("\t" + confusionMatrix[i][j]);
            bw.write("\t|\t" + confusionMatrix[i][classCnt] + "\n");
        }
        bw.write("total");
        for(int i=0; i<classCnt; i++) bw.write("\t" + confusionMatrix[classCnt][i]);
        bw.write("\n");
        bw.write("\n");

        bw.write("# Detailed Accuracy" + "\n");
        bw.write("Class\tTP_Rate\tFP_Rate\tPrecision\tRecall\tF-Measure\n");
        for(int i=0; i<classCnt; i++)
        {
            bw.write(uniqClassList.get(i) + "\t");
            bw.write(String.format("%1.3f", tpRate[i]) + "\t");
            bw.write(String.format("%1.3f", fpRate[i]) + "\t");
            bw.write(String.format("%1.3f", precision[i]) + "\t");
            bw.write(String.format("%1.3f", recall[i]) + "\t");
            bw.write(String.format("%1.3f", f1measure[i]) + "\n");
        }
        bw.write("Weig.Avg.\t");
        bw.write(String.format("%1.3f", tpRate[classCnt]) + "\t");
        bw.write(String.format("%1.3f", fpRate[classCnt]) + "\t");
        bw.write(String.format("%1.3f", precision[classCnt]) + "\t");
        bw.write(String.format("%1.3f", recall[classCnt]) + "\t");
        bw.write(String.format("%1.3f", f1measure[classCnt]) + "\n");



        bw.close();
        fout.close();


    }
}
