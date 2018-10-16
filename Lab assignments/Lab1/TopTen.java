package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {

	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5,
			xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}
		return map;
	}

	//Class to implement the map function.
	public static class TopTenMapper extends
	 Mapper<Object, Text, NullWritable, Text> {

		// Stores a map of user reputation.
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		// The map function received the whole users.xml in the Text value.
		// However, MapReduce handle everything in a "parallel" way. Then,
		// the valuethat we will receive here,it will actually be only one
		//  row of the xml file.
		public void map(Object key, Text value, Context context) throws
		IOException, InterruptedException {

			// First we need to use the transformXmlToMap function:
			// The map method will get the users.xml as a single string: value.
			Map<String, String> mapString = transformXmlToMap(value.toString());

			//As it is only one id-reputation pair, we just need to
			// read it by "Id" and "Reputation"
			String userString = mapString.get("Id");
			String reputationString = mapString.get("Reputation");

			// Skip the rows that do not contain the Id.
			if (userString == null) {
				return;
			}

			// Convert to integer and text.
			Integer reputationInt = Integer.parseInt(reputationString);

			// Now we need to put it into the TreeMap, with Integer and Text.
			// TreeMap needs the reputation as a key. However, as a Text, we
			// are sending the whole information. So, being the value that
			// we received through the map function.
			repToRecordMap.put(reputationInt, new Text(value));

			// We only want the ten top records so we remove the last key,
			// given it is in descending order.
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			// Output our ten records to the reducers with a null key.
			// The ten outputs that we will send will contain the whole
			// information: being the id and the resolution part of it.
			for (Text information : repToRecordMap.values()) {
				context.write(NullWritable.get(), information);
			}
		}
	}

	public static class TopTenReducer extends
	TableReducer<NullWritable, Text, NullWritable> {

		// Stores a map of user reputation to the record.
		private TreeMap<Integer, Text> repToRecordMap =
		new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {

			try{
			// We only have one reducer so we can iterate for the whole
			// Text values and save it into our HBase Table structure.

				// The reducer needs to store again the 10 received values
				// into a TreeMap.
				for(Text user_value: values){

					Map<String, String> mapString =
					transformXmlToMap(user_value.toString());

					String userString = mapString.get("Id");
					String reputationString = mapString.get("Reputation");

					// Convert to integer and text.
					Integer reputationInt = Integer.parseInt(reputationString);
					Text userText = new Text(userString);

					// In this tree, we will save the reputation as a key,
					// and the user id as the value.
					repToRecordMap.put(reputationInt, userText);

					if (repToRecordMap.size() > 10) {
						repToRecordMap.remove(repToRecordMap.firstKey());
					}

				}

				Integer count = 0;

				// Going to the TreeMap:
				for (Integer data: repToRecordMap.descendingKeySet()){

					// Create Hbase put.
					Put insHBase = new Put(Integer.toString(count).getBytes());

					//Add the particular columns (rep and id).
					insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"),
					Integer.toString(data).getBytes());
					insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"),
					repToRecordMap.get(data).getBytes());

					// Write data to Hbase Table.
					context.write(NullWritable.get(), insHBase);
					count += 1;
				}

			} catch(Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception {

		// Configure new job.
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf,"Topten reputation");
		job.setJarByClass(TopTen.class);

		// Set the Mapper, Output- and Reduce classes.
		job.setMapperClass(TopTenMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(TopTenReducer.class);
		job.setNumReduceTasks(1);

		//Input file: users.xml
		FileInputFormat.addInputPath(job, new Path(args[0]));

		//Output: Define HBase Table.
		TableMapReduceUtil.initTableReducerJob("topten",
		TopTenReducer.class, job);

		job.waitForCompletion(true);


	}
}
