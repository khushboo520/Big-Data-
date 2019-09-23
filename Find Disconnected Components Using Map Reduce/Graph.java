import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;      // the vertex neighbors
	public long noOfNeighbors;
	
	Vertex(){
		this.tag=0;
    	this.VID=0;
    	this.group=0;		
    	this.adjacent=new Vector<Long>();
	}
	Vertex(short tag,long group,long VID,Vector<Long> adjacent){
		this.tag=tag;
		this.group=group;
		this.VID=VID;
		this.adjacent=adjacent;
		
		
	}
	Vertex(short tag, long group){
		this.tag=tag;
		this.group=group;
		this.VID=0;
		this.adjacent = new Vector<Long>();
	}
	
	
     public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
         out.writeLong(group);
		 out.writeLong(VID);
		LongWritable adjacentsize = new LongWritable(adjacent.size());
        adjacentsize.write(out);
		for(long neighbor:adjacent)
		{
			 System.out.println("vid"+VID+" neighbor "+neighbor);
			out.writeLong(neighbor);
		}
		
		
	}

	
	public void readFields ( DataInput in ) throws IOException {
		
        tag = in.readShort();
        group = in.readLong();
		VID=in.readLong();
		
		adjacent=new Vector<Long>();
		LongWritable noOfNeighbors = new LongWritable();
    	noOfNeighbors.readFields(in);
		
		for(long j=0;j<noOfNeighbors.get();j++){
			long value = in.readLong();
			System.out.println("vid read :"+VID+" n "+value);
			//if(value!=null)
				adjacent.add(value);
		}
		
    }
}

public class Graph {

   


 static class GraphNodeMapper extends Mapper<Object,Text,LongWritable,Vertex > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");	
		
			Vector<Long> adjacent =new Vector<Long>();
			long verterxId=s.nextLong();
			while(s.hasNext())
				{
					long a=s.nextLong();
					
					adjacent.add(a);
				}
				
			
			 Vertex node=new Vertex((short)0,verterxId,verterxId,adjacent);
			 System.out.println("vid"+verterxId+" size "+adjacent.size());
             context.write(new LongWritable(verterxId),node);
            s.close();
        }
}
 
	
	static class UniqueGroupMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
                       context.write(new LongWritable(value.VID),value);
					    System.out.println("M2 vertex "+value.VID+" key "+key);
           for(Long adjectNode:value.adjacent){
			   System.out.println("for each adacent "+adjectNode+" group is "+value.group);
			    context.write(new LongWritable(adjectNode),new Vertex((short)1,value.group));
		   }
        }
    }
	
	static class UniqueGroupReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
		
        
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
							   System.out.println("reducer for "+key);
            long minimumGroupNumber=Long.MAX_VALUE;
			Vector<Long> adjacent=new Vector<Long>();
			for(Vertex node:values){
				if(node.tag==0){
					 System.out.println("reducer: adjacents "+node.VID);

				adjacent=(Vector)node.adjacent.clone();  
				} 
				
				minimumGroupNumber=(minimumGroupNumber<=node.group)?minimumGroupNumber:node.group;
				System.out.println("new group "+minimumGroupNumber);
			}
			System.out.println("emit group "+minimumGroupNumber +" for vertex "+key+" adjacent size "+adjacent.size());
			context.write(new LongWritable(minimumGroupNumber),new Vertex((short)0,minimumGroupNumber,(Long)key.get(),adjacent));
			
        }
    }
	
	
	static class CountConnectedComponentMapper extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> {
        @Override
        public void map ( LongWritable group, Vertex value, Context context )
                        throws IOException, InterruptedException {
                                 
			    context.write(group,new LongWritable(1));
		   
        }
    }
	
	static class CountConnectedComponentReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        
        @Override
        public void reduce ( LongWritable groupNumber, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException
		{
            long noOfCounts=0;
			
			for(LongWritable v:values)
			{
				noOfCounts=noOfCounts+v.get();
				
			}
			context.write(groupNumber,new LongWritable(noOfCounts));
			 
   
        }
    }
	
	
    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("GraphFirstJob");
		job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
		job.setMapperClass(GraphNodeMapper.class);
        //job.setReducerClass(ResultReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
         FileInputFormat.setInputPaths(job,new Path(args[0]));
         FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        /* ... First Map-Reduce job to read the graph */
        boolean firstJobSuccess=job.waitForCompletion(true);
		boolean secondJobSuccess=false;
		if(firstJobSuccess)
		{
				for ( short i = 0; i < 5; i++ ) 
				{
					job = Job.getInstance();
					/* ... Second Map-Reduce job to propagate the group number */
					job.setJobName("GroupMemberJob");
				job.setJarByClass(Graph.class);
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Vertex.class);
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(Vertex.class);
				job.setMapperClass(UniqueGroupMapper.class);
				job.setReducerClass(UniqueGroupReducer.class);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);		
				//job.setOutputFormatClass(TextOutputFormat.class);
				 FileInputFormat.setInputPaths(job,new Path(args[1]+"/f"+i));
				 FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f"+(i+1)));
				
				 secondJobSuccess=job.waitForCompletion(true);
					 if(!secondJobSuccess)
					 {
						 break;
					 }
				}
		}
		if(secondJobSuccess)
		{
			job = Job.getInstance();
			/* ... Final Map-Reduce job to calculate the connected component sizes */
			job.setJobName("GraphVertecCountJob");
			job.setJarByClass(Graph.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Vertex.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setMapperClass(CountConnectedComponentMapper.class);
			job.setReducerClass(CountConnectedComponentReducer.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			//job.setOutputFormatClass(SequenceFileOutputFormat.class);		
			job.setOutputFormatClass(TextOutputFormat.class);
			 FileInputFormat.setInputPaths(job,new Path(args[1]+"/f5"));
			 FileOutputFormat.setOutputPath(job,new Path(args[2]));
			job.waitForCompletion(true);
		}
    }
}