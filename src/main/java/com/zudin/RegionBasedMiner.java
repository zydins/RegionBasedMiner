package com.zudin;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

/**
 * Sergey Zudin
 * Date: 13.04.14
 */
public class RegionBasedMiner {
    private static ArrayList<LogCase> cases = new ArrayList<LogCase>();
    private static PetriNet net = PetriNet.getInstance();

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            System.out.println("[ERROR] Paths isn't set.\n" +
                    "Example of using: hadoop jar RBM.jar input [output]\n" +
                    "\tinput - input path (/logs/temp.log)\n" +
                    "\toutput - output path (/results/temp1.txt)");
            System.exit(1);
        }
        String inputLogPath = "";// = "/logs/demo2.txt";
        String outputLogPath = "";// = "/Users/vaultboy/Dropbox/Study/CW3/new/data/result2";
        if (args.length == 1) {
            inputLogPath = args[0];
            outputLogPath = "/RBM_results/run" + System.currentTimeMillis() + "";
            System.out.println("[INFO] Results will be saved in \"" + outputLogPath + "\"");
        }
        if (args.length == 2) {
            inputLogPath = args[0];
            outputLogPath = args[1];
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path input = new Path(inputLogPath);
        Path output = new Path(outputLogPath);
        if (!fs.exists(input)) {
            System.out.println("[ERROR] Input file (" + inputLogPath + ") doesn't exist");
        }
        if (fs.exists(output)) {
            outputLogPath = "/RBM_results/run" + System.currentTimeMillis() + "";
            output = new Path(outputLogPath);
            System.out.println("[WARN] Output directory already exist. Results will be saved in " + outputLogPath + "\"");
        }

        JobClient.runJob(runLogParseJob(input, output));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputLogPath + "/part-00000"))));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] arr = line.split("\t");
            cases.add(new LogCase(Integer.parseInt(arr[0]), arr[1]));
            net.addSequence(arr[1]);
        }
        reader.close();


        LogCase result = cases.get(0).mergeSolutions(cases.get(1));
        for (int i = 2; i < cases.size(); i++) {
            result = result.mergeSolutions(cases.get(i));
        }





        for (int[] arr : result.getSolutions()) {
            if (Methods.sumOfArrElem(arr) > 1 /*&& Methods.sumOfArrElem(arr) <= length*/) { //there max length of possible log
                if (arr[0] != 1) { //Условие 1. Ограничение на стартовый регион
                    //Condition 2. Usual place must contain in (x) and out (y) transitions
                    List<Integer> enabledActivitiesIndexes = getIndexesOfOnes(arr);
                    List<String> enabledActivitiesNames = new ArrayList<String>();
                    String activity = result.getActivities().get(enabledActivitiesIndexes.get(0));
                    int check = activity.startsWith("x")?1:2;
                    enabledActivitiesNames.add(activity);
                    for (int i = 1; i < enabledActivitiesIndexes.size(); i++) {
                        activity = result.getActivities().get(enabledActivitiesIndexes.get(i));
                        enabledActivitiesNames.add(activity);
                        if ((activity.startsWith("x") && check != 1) || (activity.startsWith("y") && check != 2)) {
                            check = 0;
//                            break;
                        }
                    }
                    if (check != 0) {
                        continue;
                    }
                    //Условие 3. Принадлежат одному логу
                    if (enabledActivitiesNames.contains("xc") && enabledActivitiesNames.contains("xb") && enabledActivitiesNames.contains("ye")
                            && enabledActivitiesNames.size() == 3) {
                        System.out.print('1');
                    }

                    Set<LogCase> parents = result.getParents();
                    MultiValueMap map = new MultiValueMap();
                    MultiValueMap mapNames = new MultiValueMap();
//                    HashMap<String, ArrayList<int[]>> map = new HashMap<String, ArrayList<int[]>>();
                    boolean equals = false;
                    boolean redundance = false;
                    Collections.sort(enabledActivitiesNames);
                    for (LogCase parent : parents) {     //adb????
                        List<String> parAct = new ArrayList<String>(parent.getActivities());
                        parAct.retainAll(enabledActivitiesNames);
//                        Collections.sort(enabledActivitiesNames);
                        if (parAct.equals(enabledActivitiesNames)) { //полное совпадение
                            equals = true;
                            List<Integer> indexes = new ArrayList<Integer>();
                            for (String act : parAct) {
                                indexes.add(parent.getActivities().indexOf(act));
                            }
                            //проверка на длину лога (если длинный, то удалим переход между не соседними активити)
                            redundance = net.getNumOfActivities() >= 6 && Collections.max(indexes) - Collections.min(indexes) > 1;
                            break;
                        } else {
                            if (parAct.size() > 0) {  //частичное совпадение
                                for (String act : parAct) {
                                    map.put(parent.getId(), new Object[] {act, parent.getActivities().indexOf(act)}); //Добавляем id и index
                                    mapNames.put(act, parent.getId());
                                }
                            }
                        }
                    }
                    //Условие 4.  Логи имеют смежные активити  (???)
                    if (!equals) { //проверка на активити нужна? (x_ или y_)
                        boolean adjacent = true;
                        //int max = 0;
                        //int min = Integer.MAX_VALUE;
                        //int i = 0;
                        //int[] lengths = new int[map.size()];
                        List<String> checkList = new ArrayList<String>(enabledActivitiesNames);
                        for (Object key : map.keySet()) {
                            List<Integer> indexes = new ArrayList<Integer>();
                            List<String> names = new ArrayList<String>();
                            for (Object value : map.getCollection(key)) {
                                Object[] pair = (Object[]) value;
                                indexes.add((Integer) pair[1]);
                                names.add((String) pair[0]);
                            }
                            if (names.size() == 1/* && mapNames.getCollection(names.get(0)).size() > 1*/) {  //для примера 7
                                boolean excess = false;
                                for (Object id : mapNames.getCollection(names.get(0))) { //get ids
                                    if (map.getCollection(id).size() > 1) {
                                        excess = true;
                                        break;
                                    }
                                }
                                if (excess) continue;
                            }
                            checkList.retainAll(names);
                            //lengths[i++] = indexes.size();
                            if (/* indexes.size() < 2 || */(Collections.max(indexes) - Collections.min(indexes) > 1 )) { //недопустимый разрыв или длинна
                                adjacent = false;
                                break;
                            }
                            //max = Collections.max(indexes) > max ? Collections.max(indexes) : max;
                            //min = Collections.min(indexes) < min ? Collections.min(indexes) : min;
                        }
//                        if (!adjacent || (max - min > 1)) continue;
                        if (!adjacent || checkList.isEmpty()) continue;
                    }
                    if (!redundance) {
                        net.addPlace(enabledActivitiesNames);
                    }
                }
            }
        }
        net.addPlace(LogCase.getStartPlace(cases));
        net.addPlace(LogCase.getFinalPlace(cases));
        net.makeSafe();
        System.out.println(net);
        Path resFile = new Path(outputLogPath + "/result_net.txt");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(resFile, true)));
        writer.write(net.toString());
        writer.close();

    }

    private static ArrayList<Integer> getIndexesOfOnes(int[] arr) {
        ArrayList<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == 1) {
                result.add(i);
            }
        }
        return result;
    }

    private static JobConf runLogParseJob(Path inputPath, Path outputPath) {
        JobConf job = new JobConf();
        job.setJarByClass(RegionBasedMiner.class);
        job.setJobName("RBM Log Parser");
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        return job;
    }
}
