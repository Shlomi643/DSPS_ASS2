package probCalculation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CalculateProbFormula {
    public static class TwoAndThreeGramMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(Utils.extractKey(value), Utils.extractValue(value));
        }
    }

    public static class OneAndThreeGramMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(Utils.extractKey(value), Utils.extractValue(value));
        }
    }

    public static class ThreeGramMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(Utils.extractKey(value), new Text(Utils.N3IDENTIFIER + Utils.DELIMITER + Utils.extractValue(value)));
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text text, Text text2, int i) {
            return (text.hashCode() & Integer.MAX_VALUE) % i; // Non-negative
        }
    }

    public static class CalculateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> tmps = new LinkedList<>();
            values.forEach(x -> tmps.add(x.toString()));
            context.write(key, new Text(String.join("", tmps)));
            //            System.out.println("key " + key + " values " + values);
//            System.err.println("key " + key + " values " + values);
//            Map<String, Double> formulaComponents = getFormulaComponents(values);
//            String prob = computeFormula(formulaComponents).toString();
//
//            context.write(new Text(key + Utils.DELIMITER + prob), new Text());
        }


        private Double computeFormula(Map<String, Double> formulaComponents) {
            System.out.println("formula " + formulaComponents);
            System.err.println("formula " + formulaComponents);
            double k2 = computeK2(formulaComponents.get(Utils.N2IDENTIFIER));
            double k3 = computeK3(formulaComponents.get(Utils.N3IDENTIFIER));
            double e1 = k3 * formulaComponents.get(Utils.N3IDENTIFIER) / formulaComponents.get(Utils.C2IDENTIFIER);
            double e2 = (1 - k3) * k2 * formulaComponents.get(Utils.N2IDENTIFIER) / formulaComponents.get(Utils.C1IDENTIFIER);
            double e3 = (1 - k3) * (1 - k2) * formulaComponents.get(Utils.N1IDENTIFIER) / formulaComponents.get(Utils.C0IDENTIFIER);
            return e1 + e2 + e3;
        }

        private double computeK2(double n2) {
            return (Math.log10(n2 + 1) + 1) / (Math.log10(n2 + 1) + 2);
        }

        private double computeK3(double n3) {
            return (Math.log10(n3 + 1) + 1) / (Math.log10(n3 + 1) + 2);
        }

        /**
         * @param values <Identifier actualValue>
         */
        private Map<String, Double> getFormulaComponents(Iterable<Text> values) {
            Map<String, Double> formulaComponents = new HashMap<>();
            for (Text val : values)
                formulaComponents.put(getIdentifier(val), getActualValue(val));
            return formulaComponents;
        }

        private String getIdentifier(Text value) {
            return value.toString().split(Utils.DELIMITER)[0];
        }

        private Double getActualValue(Text value) {
            return Double.parseDouble(value.toString().split(Utils.DELIMITER)[1]);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "CalculateProbFormula");
        job.setJarByClass(CalculateProbFormula.class);
        job.setReducerClass(CalculateReducer.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TwoAndThreeGramMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OneAndThreeGramMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, ThreeGramMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
