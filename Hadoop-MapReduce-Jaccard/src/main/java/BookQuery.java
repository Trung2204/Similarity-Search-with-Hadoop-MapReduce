import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.Loader;
// import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

        /*
        In Hadoop MapReduce, you don’t directly pass a folder to a mapper. Instead, you specify the input path(s) for the job, and Hadoop’s InputFormat determines how to split the input files and directories into input splits. Each input split is then processed by a separate mapper.
            Set the input path in your driver code:
                FileInputFormat.addInputPath(job, new Path("path/to/your/input/directory"));
                    In this example, replace "path/to/your/input/directory" with the path to your input directory. This can be a directory in HDFS (Hadoop Distributed File System), and it can contain multiple files. Hadoop will process all files in the specified directory and any subdirectories.
        Each file in the directory will be split into chunks (typically on block boundaries), and each chunk will be assigned to a separate mapper. If the files are small, several files can be grouped into a single split and processed by one mapper.
            In your mapper, you can access the current file’s name and path like this:
                FileSplit fileSplit = (FileSplit)context.getInputSplit();
                String filename = fileSplit.getPath().getName();

        This can be useful if you need to know which file the current key-value pair came from.

        Remember, the mapper doesn't know about the directory or other files; it only processes the key-value pairs that are passed to it from its input split. Let me know if you need further clarification or help! 😊
        */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the current document name to use as URL
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileNameAsUrl = fileSplit.getPath().getName().split("\\.")[0];

            // Split the document into words
            String[] words = value.toString().split("\\W+");
            // Create terms as 3 consecutive words
            Set<String> mapTermSet = new HashSet<>();
            for (int i = 0; i < (words.length - 2); i++) {
                String term = words[i] + " " + words[i + 1] + " " + words[i + 2];
                mapTermSet.add(term);
            }
            // Filter out terms that are not in the query
            mapTermSet.retainAll(queryTermSet);
//            Set<String> NotYetInAllTermSet = new HashSet<>(mapTermSet);
//            NotYetInAllTermSet.removeAll(allTermSet);
//            // Add the term set of mapper to the set of all terms
//            allTermSet.addAll(NotYetInAllTermSet);

            // Emit intermediate key-value pairs [term, URL@length]
            int length = mapTermSet.size();
            for (String termAsKey : mapTermSet) {
                context.write(new Text(termAsKey), new Text(fileNameAsUrl + "@" + length));
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, LongWritable, Text> {
        // auto-increment LongWritable key
        private long autoIncrement = 0;

        @Override
        protected void reduce(Text termAsKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            autoIncrement += 1;

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
                context.write(new LongWritable(autoIncrement), new Text(String.join(",", urlAndLengthList)));
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
            // Emit pairs of URLs for Jaccard similarity calculation
            for (String s : urlAndLengthArray) {
                String[] tempArr = s.split("@");
                for (String tempS : tempArr) {
                    if (!tempS.equals("query")) {
                        String oneString = "1";
                        context.write(new Text(tempS + "@query@" + queryTermsCount), new Text(oneString));
                    }
                }
                /* if (!s.split("@")[0].equals("query")) {
                    String oneString = "1";
                    context.write(new Text(s + "@query@" + queryTermsCount), new Text(oneString));
                } */
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
            double jaccard = (sum != ((len1 + len2) - sum)) ? ((double) sum / ((len1 + len2) - sum)) : 1;
            context.write(new Text(urlOrLength[0] + " - " + urlOrLength[2]), new Text(String.valueOf(jaccard)));
        }
    }

    public static void main(String[] args) throws Exception {
        // System.out.println(System.getProperty("java.version"));
        String resource = "";
        Loader.getResource(resource, Logger.class);
        System.out.println(resource);
        /* Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: <input> <intermediate output> <final output>");
            System.exit(2);
        } */
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        String input = args[0];
        String output = args[1];
        String queryPath = args[2];
        FileSystem fs = FileSystem.get(URI.create(input), conf);
        /* In Hadoop MapReduce, if the input is a folder of text files, the value argument of the mapper function will be the content of a single line from one of the text files
        The input to the mapper is split up by the InputFormat specified in the job configuration, the default being TextInputFormat, which splits up text files line by line. If you need a different way of splitting your input, you would need to use a different InputFormat. For example, WholeFileInputFormat can be used if you want each file to be an input to the mapper. */
        // Read the query file from HDFS
        Path queryFilePath = new Path(queryPath);
        FSDataInputStream inputStream = fs.open(queryFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder query = new StringBuilder();
        String line = reader.readLine();
        while (line != null){
            query.append(line).append(" ");
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

        // System.out.println("This is a query: " + query);
        // System.out.println("This is total number of documents: " + fileCount);

        // Configure and run the first MapReduce job
        Job job1 = Job.getInstance(conf, "Jaccard Similarity - Job 1");
        job1.setJarByClass(BookQuery.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(input));
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