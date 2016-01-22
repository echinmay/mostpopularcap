package com.chinmay.cloudcapstone.mostpopular;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * Hello world!
 *
 */
public class MostPopular extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MostPopular(), args);
        System.exit(res);
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/home/chinmay/22");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Airport Stat");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(LongWritable.class);

        jobA.setMapperClass(AirportArrDepMapper.class);
        jobA.setReducerClass(AirportArrDepReducer.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(MostPopular.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Airport");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(LongWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopAirportMapper.class);
        jobB.setReducerClass(TopAirportReducer.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(MostPopular.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class AirportArrDepMapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, "|");
            int tokenNum=0;
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken().trim();
                if ((tokenNum == 1) || (tokenNum == 3)) {
                    //if (this.swords.get(nextToken.trim().toLowerCase()) != true) {
                    context.write(new Text(nextToken), new LongWritable(1));
                }
                tokenNum++;
            }
        }
    }

    public static class AirportArrDepReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class TopAirportMapper extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Long, String>> countToAirportMap = new TreeSet<Pair<Long, String>>();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Long count = Long.parseLong(value.toString());
            String airport = key.toString();

            countToAirportMap.add(new Pair<Long, String>(count, airport));

            if (countToAirportMap.size() > 10) {
                countToAirportMap.remove(countToAirportMap.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Long, String> item : countToAirportMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopAirportReducer extends Reducer<NullWritable, TextArrayWritable, Text, LongWritable> {
        private TreeSet<Pair<Long, String>> countToAirportMap = new TreeSet<Pair<Long, String>>();
        private TreeSet<Pair<Long, String>> countToAirportMapRev = new TreeSet<Pair<Long, String>>();

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] pair = (Text []) val.toArray();
                String title = pair[0].toString();
                Long count = Long.parseLong(pair[1].toString());

                countToAirportMap.add(new Pair<Long, String>(count, title));

                if (countToAirportMap.size() > 10) {
                    countToAirportMap.remove(countToAirportMap.first());
                }
            }
            countToAirportMapRev = (TreeSet<Pair<Long, String>>) countToAirportMap.descendingSet();

            for (Pair<Long, String> item: countToAirportMapRev) {
                Text word = new Text(item.second);
                LongWritable value = new LongWritable(item.first);
                context.write(word, value);
            }
        }

    }
}


// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
