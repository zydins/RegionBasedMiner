package com.zudin;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Sergey Zudin
 * Date: 13.04.14
 */
public class RegionBasedMiner {
    //public static HashMap<Integer, ArrayList<int[]>> solutionsById = new HashMap<Integer, ArrayList<int[]>>();
    static ArrayList<LogCase> cases = new ArrayList<LogCase>();
    static PetriNet net = PetriNet.getInstance();
    static int length = 4;

    public static void main(String... args) throws Exception {

        String inputLogPath = "/Users/vaultboy/Dropbox/Study/CW3/new/data/demo2.txt";
        String outputLogPath = "/Users/vaultboy/Dropbox/Study/CW3/new/data/result2";
        JobClient.runJob(runLogParseJob(inputLogPath, outputLogPath));
        BufferedReader br = new BufferedReader(new FileReader(outputLogPath + "/part-00000"));
        String line;
        while ((line = br.readLine()) != null) {
            String[] arr = line.split("\t");
            cases.add(new LogCase(Integer.parseInt(arr[0]), arr[1]));
            net.addSequence(arr[1]);
        }
        br.close();


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

    private static JobConf runLogParseJob(String inputPath, String outputPath) throws Exception {
        JobConf job = new JobConf();
        job.setJarByClass(RegionBasedMiner.class);
        job.setJobName("RBM Log Parser");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        return job;
    }
}
