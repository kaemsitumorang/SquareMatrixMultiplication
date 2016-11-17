import java.io.IOException;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {
	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text outputkey = new Text();
			Text outputvalue = new Text();
			int m = 1000;
			while (itr.hasMoreTokens()) {
				String[] temp = itr.nextToken().split(",");
				if(temp[0].equals("1")){
					for(int x = 0; x < m; x++){
						outputkey.set(temp[1] + "," + x);
						outputvalue.set(temp[0] + "," + temp[2] + "," + temp[3]);
						context.write(outputkey, outputvalue);
					}
				}
				else{
					for(int i = 0; i < m; i++){
						outputkey.set(i + "," + temp[2]);
						outputvalue.set("2," + temp[1] + "," + temp[3]);
						context.write(outputkey, outputvalue);
					}
				}
			}
		}
	}
	
public static class Combiner extends Reducer<Text, Text, Text, Text> {
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	      Set<Text> uniques = new HashSet<Text>();
	      for (Text value : values) {
	        if (uniques.add(value)) {
	          context.write(key, value);
	        }
	      }
	    }
}

public static class MyReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		String[] value;
		HashMap<Integer, Float> hashBaris = new HashMap<Integer, Float>();
		HashMap<Integer, Float> hashKolom = new HashMap<Integer, Float>();
			for (Text val : values) {
				value = val.toString().split(",");
				if(value[0].equals("1")){
					hashBaris.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
				else{
					hashKolom.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
			}
			int n = 1000;
			float result = 0.0f;
			float baris;
			float kolom;
			for (int j = 0; j < n; j++){
				baris = hashBaris.containsKey(j) ? hashBaris.get(j) : 0.0f;
				kolom = hashKolom.containsKey(j) ? hashKolom.get(j) : 0.0f;
				result += baris * kolom; 
			}
			if(result != 0.0f){
				context.write(key, new Text(Float.toString(result)));
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "matriks");
		job.setJarByClass(MatrixMultiplication.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}