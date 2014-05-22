package com.zudin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.*;
import sun.dc.path.PathException;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.*;

/**
 * Sergey Zudin
 * Date: 13.04.14
 */
public class RegionBasedMiner {
    private static ArrayList<LogCase> cases = new ArrayList<LogCase>();
    private static PetriNet net = PetriNet.getInstance();
    private static String inputLogPath = "";
    private static String outputLogPath = "";

    /**
     * Test executor
     * @param args paths to files
     */
    public static void main(String... args) {
        try {
            net = run(args);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = net.getXml();
            StreamResult streamResult = new StreamResult(new File("/Users/vaultboy/text.pflow"));
            transformer.transform(source, streamResult);
        } catch (Exception e) {
            System.exit(0);
        }
    }

    /**
     * Runs Region-based Miner and returns Petri net
     * @param args paths to files
     * @return Petri net
     * @throws IOException when errors with write/read is occurred
     * @throws PathException when path to input file is incorrect
     */
    public static PetriNet run(String... args) throws IOException, PathException {
        FileSystem fs = FileSystem.get(new Configuration());
        readLogs(fs, args);
        LogCase result = cases.get(0).mergeSolutions(cases.get(1));
        for (int i = 2; i < cases.size(); i++) {
            result = result.mergeSolutions(cases.get(i));
        }
        writeSolutions(fs, result);
        writeParents(fs, result);
        StringBuffer buff = new StringBuffer();
        for (String str : result.getActivities()) {
            buff.append(str);
            buff.append(" ");
        }
        JobClient.runJob(runActivitiesCheckJob(new Path(outputLogPath + "/tmp/solutions.tmp"), new Path(outputLogPath + "/actcheck"), buff.toString()));
        readCorrectPlaces(fs);
        net.addPlace(LogCase.getStartPlace(cases));
        net.addPlace(LogCase.getFinalPlace(cases));
        net.makeSafe();
        System.out.println(net);
        writeResults(fs, net);
        return net;
    }

    /**
     * Creates a Mapreduce job that parses input file to log queues
     * @param inputPath path to an input file
     * @param outputPath path where results will be saved
     * @return Job that will parse an input file
     */
    private static JobConf getLogParseJob(Path inputPath, Path outputPath) {
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

    /**
     * Creates a Mapreduce job that checks each activity list and adds it (if it is correct) as place in the Petri net
     * @param inputPath path to an file with lists
     * @param outputPath path where results will be saved
     * @param activities all possible activities
     * @return Job that will check activity lists
     */
    private static JobConf runActivitiesCheckJob(Path inputPath, Path outputPath, String activities) {
        JobConf job = new JobConf();
        job.setJarByClass(RegionBasedMiner.class);
        job.setJobName("RBM Activities Check");
        job.set("path", outputLogPath);
        job.set("num", String.valueOf(net.getNumOfActivities()));
        job.set("activities", activities);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(SolutionsMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    /**
     *
     * @param fs
     * @throws IOException
     */
    private static void readCorrectPlaces(FileSystem fs) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputLogPath + "/actcheck/part-00000"))));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] arr = line.split("\t");
            List<String> place = new ArrayList<String>();
            Collections.addAll(place, arr[1].split(" "));
            net.addPlace(place);
        }
        reader.close();
    }

    private static void readLogs(FileSystem fs, String[] args) throws IOException, PathException {
        if (args.length == 0) {
            System.out.println("[ERROR] Paths isn't set.\n" +
                    "Example of using: hadoop jar RBM.jar input [output]\n" +
                    "\tinput - input path (/logs/temp.log)\n" +
                    "\toutput - output path (/results/temp1.txt)");
            throw new PathException("Paths isn't set");
        }

        if (args.length == 1) {
            inputLogPath = args[0];
            outputLogPath = "/RBM_results/run" + System.currentTimeMillis() + "";
            System.out.println("[INFO] Results will be saved in \"" + outputLogPath + "\"");
        }
        if (args.length == 2) {
            inputLogPath = args[0];
            outputLogPath = args[1];
        }

        Path input = new Path(inputLogPath);
        Path output = new Path(outputLogPath);
        if (!fs.exists(input)) {
            System.out.println("[ERROR] Input file (" + inputLogPath + ") doesn't exist");
            throw new PathException("Input file (" + inputLogPath + ") doesn't exist");
        }
        if (fs.exists(output)) {
            outputLogPath = "/RBM_results/run" + System.currentTimeMillis() + "";
            System.out.println("[WARNING] Output directory already exist. Results will be saved in " + outputLogPath + "\"");
        }

        JobClient.runJob(getLogParseJob(input, new Path(outputLogPath + "/logparse")));

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputLogPath + "/logparse/part-00000"))));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] arr = line.split("\t");
            cases.add(new LogCase(Integer.parseInt(arr[0]), arr[1]));
            net.addSequence(arr[1]);
        }
        reader.close();
    }

    private static void writeSolutions(FileSystem fs, LogCase result) throws IOException {
        Path pathToSolutions = new Path(outputLogPath + "/tmp/solutions.tmp");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(pathToSolutions, true)));
        StringBuffer buff = new StringBuffer();
        for (int[] arr : result.getSolutions()) {
            for (int i : arr) {
                buff.append(i);
                buff.append(" ");
            }
            buff.append("\n");
        }
        writer.write(buff.toString());
        writer.close();
    }

    private static void writeParents(FileSystem fs, LogCase result) throws IOException {
        Path pathToParents = new Path(outputLogPath + "/tmp/parents.tmp");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(pathToParents, true)));
        StringBuffer buff = new StringBuffer();
        for (LogCase parent : result.getParents()) {
            buff.append(parent.getId());
            buff.append("\t");
            for (String activities : parent.getActivities()) {
                buff.append(activities);
                buff.append(" ");
            }
            buff.append("\n");
        }
        writer.write(buff.toString());
        writer.close();
    }

    private static void writeResults(FileSystem fs, PetriNet net) throws IOException {
        Path resFile = new Path(outputLogPath + "/result_net.txt");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(resFile, true)));
        writer.write(net.toString());
        writer.close();
    }
}
