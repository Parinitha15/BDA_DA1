import java.util.*;
import java.io.*;        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
        
public class pari22mis1026{
  public static class Map extends Mapper<LongWritable,Text,Text,Text> {
    Path[] cfile=new Path[0];
    ArrayList<Text> empl=new ArrayList<Text>();
    public void setup(Context context)
    {
    	Configuration conf=context.getConfiguration();
    	try
    	{
    	    	cfile = DistributedCache.getLocalCacheFiles(conf);
    	BufferedReader reader=new BufferedReader(new FileReader(cfile[0].toString()));
    	String line;
    	while ((line=reader.readLine())!=null)
    	{
    		Text tt=new Text(line);
    	empl.add(tt);	
    	}
    	    }
    	
    	catch(IOException e)
    	{
    		e.printStackTrace();
    		    	}} 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line2 = value.toString();
        String[] elements=line2.split(",");
        double netPay =0,tax =0,allowance =0;
        for(Text e:empl)
        {
        	String[] line1 = e.toString().split(",");
        	if(elements[3].equals(line1[0]))
        	{
        	   allowance = (Integer.parseInt(line1[0])*Integer.parseInt(line1[1]))/100;
        	   tax = (Integer.parseInt(line1[0])*Integer.parseInt(line1[2]))/100;
        	   netPay = Integer.parseInt(line1[0]) + allowance - tax;
        	}
            }
            
            context.write(new Text(elements[2]),new Text(""+netPay));  
  }
  }
   
  public static class P extends Partitioner<Text,Text>{
    public int getPartition(Text key,Text val,int num){
      if("Scope".equals(key.toString())) return 0;
      else if("Sense".equals(key.toString())) return 1;
      else return 2;
    }
  }
  
  public static class R extends Reducer<Text,Text,Text,Text>{
    private String k;
    private int c=0;
    public void reduce(Text key,Iterable<Text> val,Context context) throws IOException,InterruptedException{
      k=key.toString();
      for(Text v:val)
      if(Double.parseDouble(v.toString())>50000) c++;
      
    }
    protected void cleanup(Context context) throws IOException,InterruptedException{
       context.write(new Text(k),new Text(Integer.toString(c)));
    }
  } 
	    
  public static void main(String[] args) throws Exception{
	  
	   Configuration conf = new Configuration();
	Job job = new Job(conf, "distcache");
	job.setJarByClass(pari22mis1026.class);
	DistributedCache.addCacheFile(new Path(args[0]).toUri(),job.getConfiguration());
	FileInputFormat.addInputPath(job, new Path(args[1]));
	  FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		 job.setNumReduceTasks(3);
		 job.setReducerClass(R.class);
	     	job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);   
	    job.setPartitionerClass(P.class);
	  job.waitForCompletion(true);
	   }    
}
