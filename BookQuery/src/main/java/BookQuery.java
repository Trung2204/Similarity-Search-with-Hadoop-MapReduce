import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.log4j.BasicConfigurator;
//import org.apache.log4j.Logger;
// import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
// import java.util.Collections;


public class BookQuery {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private final Set<String> queryTermSet = new HashSet<>();
        // private final Set<String> allTermSet = new HashSet<>();

        // setup is called ONCE for each mapper, then all calls to the map func is made
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read the query terms from the configuration and store them in a set
            String query = context.getConfiguration().get("query");
            // Split the query into words
            String[] queryWords = query.split("\\W+"); // String[] queryTermsArray = query.split("[, ?.\n]+");
            // Create terms as 3 consecutive words
            for (int i = 0; i < queryWords.length - 2; i++) {
                String term = queryWords[i] + " " + queryWords[i + 1] + " " + queryWords[i + 2];
                queryTermSet.add(term);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Get the current document name from the path to use as URL
            /* FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileNameAsUrl = fileSplit.getPath().getName().split("\\.")[0]; */
            String[] parts = value.toString().split("/+");
            String fileNameAsUrl = parts[parts.length - 1].split("\\.")[0];

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path textPath = new Path(value.toString());
            FSDataInputStream inputStream = fs.open(textPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = reader.readLine();
            String text = "";
            while (line != null){
                text += line + " ";
                line = reader.readLine();
            }

            // Split the document into words
            String[] words = text.split("\\W+");
            // Create terms as 3 consecutive words
            Set<String> mapTermSet = new HashSet<>();
            for (int i = 0; i < (words.length - 2); i++) {
                String term = words[i] + " " + words[i + 1] + " " + words[i + 2];
                mapTermSet.add(term);
            }

            // Filter out terms that are not in the query
            /* Set<String> NotYetInAllTermSet = new HashSet<>(mapTermSet);
            NotYetInAllTermSet.removeAll(allTermSet);
            // Add the term set of mapper to the set of all terms
            allTermSet.addAll(NotYetInAllTermSet); */
            mapTermSet.retainAll(queryTermSet);

            // Emit intermediate key-value pairs [term, URL@length]
            int length = mapTermSet.size();
            for (String termAsKey : mapTermSet) {
                context.write(new Text(termAsKey), new Text(fileNameAsUrl + "@" + length));
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        // auto-increment LongWritable key
        // private long autoIncrement = 0;

        @Override
        protected void reduce(Text termAsKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // autoIncrement += 1;

            // Read input
            List<String> urlAndLengthList = new ArrayList<>();
            for (Text value : values) {
                urlAndLengthList.add(value.toString());
            }

            // Get the number of total documents (this should be set somewhere in the context configuration)
            int n = context.getConfiguration().getInt("totalDocuments", 0);

            // Check if the length of the group is neither n nor 1
            if (urlAndLengthList.size() != n && urlAndLengthList.size() != 1) {
                // Sort the group
                urlAndLengthList.sort((a, b) -> Integer.compare(Integer.parseInt(b.split("@")[1]), Integer.parseInt(a.split("@")[1])));

                // Emit the term and sorted group
                // context.write(new Text(termAsKey), new Text(String.join(",", urlAndLengthList)));
                context.write(new Text(termAsKey), new Text(String.join(",", urlAndLengthList)));
            }
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
        private final Set<String> queryTermSet = new HashSet<>();
        private String queryTermsCount;

        // setup is called ONCE for each mapper, then all calls to the map func is made
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read the query terms from the configuration and store them in a set
            String query = context.getConfiguration().get("query");
            // Split the query into words
            String[] queryWords = query.split("\\W+");
            // Create terms as 3 consecutive words
            for (int i = 0; i < (queryWords.length - 2); i++) {
                String term = queryWords[i] + " " + queryWords[i + 1] + " " + queryWords[i + 2];
                queryTermSet.add(term);
            }
            queryTermsCount = Integer.toString(queryTermSet.size()); // String queryTermsCount = String.valueOf(queryTerms.size());
        }

        @Override
        protected void map(LongWritable autoIncrement, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input value to get the list of URLs
            String[] urlAndLengthArray = value.toString().split("\t")[1].split(",");
            // String[] urlAndLengthArray = value.toString().split(",");

            // Emit pairs of URLs for Jaccard similarity calculation
            String oneString = "1";
            for (String urlAndLength : urlAndLengthArray) {
                if (!urlAndLength.split("@")[0].equals("query")) {
                    context.write(new Text(urlAndLength + "@query@" + queryTermsCount), new Text(oneString));
                }
            }
        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
                // basically sum += 1;
            }

            // Parse the URLs and lengths from the key
            String[] urlOrLength = key.toString().split("@");
            int len1 = Integer.parseInt(urlOrLength[1]);
            int len2 = Integer.parseInt(urlOrLength[3]);

            // Calculate the Jaccard similarity
            double jaccard = (sum != (len1 + len2)) ? ((double) sum / ((len1 + len2) - sum)) : 1;
            context.write(new Text(urlOrLength[0] + " - " + urlOrLength[2]), new Text(String.valueOf(jaccard)));
        }
    }

    public static void main(String[] args) throws Exception {
        // System.out.println(System.getProperty("java.version"));
        /* Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: <input> <intermediate output> <final output>");
            System.exit(2);
        } */
        // BasicConfigurator.configure();

        if (args.length != 3) {
            System.err.println("Usage: <input> <intermediate output> <final output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        String input = args[0];
        String output = args[1];
        String queryPath = args[2];

        // FileSystem fs = FileSystem.get(URI.create(input), conf);
        FileSystem fs = FileSystem.get(URI.create(input), conf);

        // Read the query file from HDFS
        Path queryFilePath = new Path(queryPath);
        FSDataInputStream inputStream = fs.open(queryFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = reader.readLine();
        String query = "";
        while (line != null){
            query += line + " ";
            line = reader.readLine();
        }
        // Add query to the configuration
        conf.set("query", query.toString());
        // closing resources
        reader.close();
        inputStream.close();

        // Add the file count to the configuration
        Path inputPath = new Path(input);
        ContentSummary cs = fs.getContentSummary(inputPath);
        long fileCount = cs.getFileCount();
        conf.setLong("totalDocuments", fileCount);

        // create a file that contains hdfs paths to all texts
        Path inputUrls = new Path("/inputUrls");
        FSDataOutputStream outputStream = fs.create(inputUrls);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        FileStatus[] fileStatus = fs.listStatus(new Path(input));
        for (FileStatus status : fileStatus) {
            writer.write(status.getPath().toString());
            writer.newLine();
        }
        writer.close();
        outputStream.close();

        // Configure and run the first MapReduce job
        Job job1 = Job.getInstance(conf, "Jaccard Similarity - Job 1");
        job1.setJarByClass(BookQuery.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // FileInputFormat.addInputPath(job1, new Path(input));
        FileInputFormat.addInputPath(job1, inputUrls);
        Path temp = new Path(output + "_temp");
        FileOutputFormat.setOutputPath(job1, temp);

        job1.waitForCompletion(true);
        // System.exit(job1.waitForCompletion(true) ? 0 : 1);

        // Configure and run the second MapReduce job
        Job job2 = Job.getInstance(conf, "Jaccard Similarity - Job 2");
        job2.setJarByClass(BookQuery.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, temp);
        FileOutputFormat.setOutputPath(job2, new Path(output));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}