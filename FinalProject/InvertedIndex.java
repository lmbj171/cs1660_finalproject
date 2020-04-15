import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;

public class InvertedIndex 
{
	
	public static ArrayList<String> stringSplit(String s)
	{
		ArrayList<String> result = new ArrayList<String>();
		
        StringBuffer alpha = new StringBuffer(),  
        num = new StringBuffer(), special = new StringBuffer(); 
          
        for (int i=0; i<s.length(); i++) 
        { 
            if (Character.isDigit(s.charAt(i))) 
                num.append(s.charAt(i)); 
            else if(Character.isAlphabetic(s.charAt(i))) 
                alpha.append(s.charAt(i)); 
            else
                special.append(s.charAt(i)); 
        } 
		
		result.add(alpha.toString());
		result.add(num.toString());
		return result;
	}
	
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		private Text docID = new Text();
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			docID.set(fileName);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, docID);
			}
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> results = new HashMap<String, Integer>();
			ArrayList<String> split = new ArrayList<String>();
			String word;
			String num;

			for (Text value : values) {
				int count = results.containsKey(value.toString()) ? results.get(value.toString()) : 0;
				results.put(value.toString(), count + 1);
			}
			for (HashMap.Entry<String,Integer> entry : results.entrySet()) 
			{
				String s = entry.getValue().toString();
				split = stringSplit(s);
				word = split.get(0);
				num = split.get(1);
				context.write(key, new Text(word + ": " + num + "\n"));
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
        if (args.length != 2) {
            System.err.println("Usage: Inverted Indices <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(InvertedIndex.class);
        job.setJobName("Inverted Indices");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
		
		Path deletePath = new Path(args[1] + "/_SUCCESS");
		
		Configuration config = new Configuration();
		FileSystem gsfs = deletePath.getFileSystem(config);
		gsfs.delete(deletePath, false);
		Path srcPath = new Path(args[1]);
		Path desPath = new Path("gs://dataproc-staging-us-280861026343-wpsvpkis/output.txt");
		boolean copySuccess = FileUtil.copyMerge(gsfs, srcPath, gsfs, desPath, false, config, null);
		if(copySuccess)
			System.out.println("Files Merge Successful.");
		else
			System.out.println("Files Merge Failed.");
    }
}