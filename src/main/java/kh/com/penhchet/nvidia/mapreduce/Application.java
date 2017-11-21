package kh.com.penhchet.nvidia.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Iterators;
import com.jayway.jsonpath.internal.function.numeric.Average;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by HP1 on 3/31/2017.
 */
public class Application {

    public static class NVDAMapper extends Mapper<LongWritable, Text, Text, NVDAWriable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	System.out.println(key);
            if(key.get() == 0 && value.toString().contains("Date")) {
            	return;
            }else{
                String line = value.toString();
                String columns[] = line.split(",");
                String date = columns[0].substring(0, columns[0].lastIndexOf("-"));
                System.out.println(columns[0]);
                context.write(new Text(date), new NVDAWriable(new DoubleWritable(new Double(columns[4])), new DoubleWritable(Double.valueOf(columns[4]))));
            }
        }
    }

    public static class NVDAReducer extends Reducer<Text, NVDAWriable, Text, NVDAWriable>{

        @Override
        protected void reduce(Text key, Iterable<NVDAWriable> values, Context context) throws IOException, InterruptedException {
            double maxValue = Double.MIN_VALUE;
            double total = 0;
            int size = 0;
            for(NVDAWriable value: values){
            	size++;
                maxValue = Math.max(maxValue, value.getMax().get());
                total += value.getMax().get();
            }
            
            context.write(key, new NVDAWriable(new DoubleWritable(maxValue), new DoubleWritable(total/size)));
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
    	System.setProperty("hadoop.home.dir", "/home/hadoop/hadoop");
        if(args.length !=2){
            System.err.println("Usage: Max and Average Close Price <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(Application.class);
        job.setJobName("NVDIA Find Maximum and Average of Close Price");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NVDAMapper.class);
        job.setReducerClass(NVDAReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NVDAWriable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
