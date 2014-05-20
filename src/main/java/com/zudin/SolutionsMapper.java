package com.zudin;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author Sergey Zudin
 * @since 19.05.14
 */
public class SolutionsMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, IntWritable, Text> {
    private static String path = "";
    private static List<String> workActivities = new ArrayList<String>();
    private static int maxNumOfActivities;

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        Map<Integer, List<String>> parentMap = readParents();

        String[] strarr= text.toString().split(" ");
        int[] arr = new int[strarr.length];
        for (int i = 0; i < strarr.length; i++) {
            arr[i] = Integer.parseInt(strarr[i]);
        }

        if (Methods.sumOfArrElem(arr) > 1) { //there max length of possible log
            if (arr[0] != 1) { //Условие 1. Ограничение на стартовый регион
                //Condition 2. Usual place must contain in (x) and out (y) transitions
                List<Integer> enabledActivitiesIndexes = Methods.getIndexesOfOnes(arr);
                List<String> enabledActivitiesNames = new ArrayList<String>();
                String activity = workActivities.get(enabledActivitiesIndexes.get(0));
                int check = activity.startsWith("x")?1:2;
                enabledActivitiesNames.add(activity);
                for (int i = 1; i < enabledActivitiesIndexes.size(); i++) {
                    activity = workActivities.get(enabledActivitiesIndexes.get(i));
                    enabledActivitiesNames.add(activity);
                    if ((activity.startsWith("x") && check != 1) || (activity.startsWith("y") && check != 2)) {
                        check = 0;
                    }
                }
                if (check != 0) {
                    return;
                }
                //Условие 3. Принадлежат одному логу
                if (enabledActivitiesNames.contains("xc") && enabledActivitiesNames.contains("xb") && enabledActivitiesNames.contains("ye")
                        && enabledActivitiesNames.size() == 3) {
                    System.out.print('1');
                }

                MultiValueMap map = new MultiValueMap();
                MultiValueMap mapNames = new MultiValueMap();
                boolean equals = false;
                boolean redundance = false;
                Collections.sort(enabledActivitiesNames);
                for (int key : parentMap.keySet()) {
                    List<String> parAct = new ArrayList<String>(parentMap.get(key));
                    parAct.retainAll(enabledActivitiesNames);
                    if (parAct.equals(enabledActivitiesNames)) { //полное совпадение
                        equals = true;
                        List<Integer> indexes = new ArrayList<Integer>();
                        for (String act : parAct) {
                            indexes.add(parentMap.get(key).indexOf(act));
                        }
                        //проверка на длину лога (если длинный, то удалим переход между не соседними активити)
                        redundance = maxNumOfActivities >= 6 && Collections.max(indexes) - Collections.min(indexes) > 1;
                        break;
                    } else {
                        if (parAct.size() > 0) {  //частичное совпадение
                            for (String act : parAct) {
                                map.put(key, new Object[] {act, parentMap.get(key).indexOf(act)}); //Добавляем id и index
                                mapNames.put(act, key);
                            }
                        }
                    }
                }
                //Условие 4.  Логи имеют смежные активити
                if (!equals) {
                    boolean adjacent = true;
                    List<String> checkList = new ArrayList<String>(enabledActivitiesNames);
                    for (Object key : map.keySet()) {
                        List<Integer> indexes = new ArrayList<Integer>();
                        List<String> names = new ArrayList<String>();
                        for (Object value : map.getCollection(key)) {
                            Object[] pair = (Object[]) value;
                            indexes.add((Integer) pair[1]);
                            names.add((String) pair[0]);
                        }
                        if (names.size() == 1) {
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
                        if (Collections.max(indexes) - Collections.min(indexes) > 1 ) { //недопустимый разрыв или длинна
                            adjacent = false;
                            break;
                        }
                    }
                    if (!adjacent || checkList.isEmpty()) return;
                }
                if (!redundance) {
                    StringBuffer buff = new StringBuffer();
                    for (String str : enabledActivitiesNames) {
                        buff.append(str);
                        buff.append(" ");
                    }
                    output.collect(new IntWritable(0), new Text(buff.toString()));
                }
            }
        }
    }

    @Override
    public void configure(JobConf job) {
        path = job.get("path");
        Collections.addAll(workActivities, job.get("activities").split(" "));
        maxNumOfActivities = Integer.parseInt(job.get("num"));
    }

    private Map<Integer, List<String>> readParents() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(path + "/tmp/parents.tmp"))));
        String line;
        Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
        while ((line = reader.readLine()) != null) {
            String[] arr = line.split("\t");
            int id = Integer.parseInt(arr[0]);
            String[] activities = arr[1].split(" ");
            List<String> parAct = new ArrayList<String>();
            Collections.addAll(parAct, activities);
            map.put(id, parAct);
        }
        reader.close();
        return map;

    }
}
