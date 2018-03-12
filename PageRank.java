package org.myorg;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.nio.charset.CharacterCodingException;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;




public class PageRank extends Configured implements Tool {

//default constructor
 public PageRank()
{
}
private static int N;  // number of files in the corpus

private static NumberFormat numberformat= new DecimalFormat("0");// Initilizing the number format

public static void main(String[] args) throws Exception {
// Main function 
System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
}

//run method for calculating Rank
private boolean RankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
 // create job for rank calulation
        Job rankCal = Job.getInstance(conf, "rankCalculator");
        rankCal.setJarByClass(PageRank.class);

        rankCal.setOutputKeyClass(Text.class);
        rankCal.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCal, new Path(inputPath));// Defining Input path
        FileOutputFormat.setOutputPath(rankCal, new Path(outputPath));// Defining Output path

        rankCal.setMapperClass(RankCalMap.class); //setting up corresponding mapper class for rank calculation
        rankCal.setReducerClass(RankCalReduce.class); // setting up corresponding reducer class for rank calculation

        return rankCal.waitForCompletion(true);
    }


   public int run(String[] args) throws Exception {
// Check that we have the number of nodes/lines/ title 
   boolean isfinish=NumberOfNodes(args);
   if (!isfinish) return 1;
   PageRank pageRank1 = new PageRank();
   isfinish =pageRank1.ruFParsing(args); // calling parsing function
   if (!isfinish) return 1;
   String lastResultPath = null;
   // Iteration for convergence
   for (int iter = 0; iter < 10; iter++) {
            String inPath = "PageRank/output" + numberformat.format(iter);
            lastResultPath = "PageRank/output" + numberformat.format(iter + 1);

            isfinish = RankCalculation(inPath, lastResultPath);

            if (!isfinish) return 1;
        }

isfinish = RankOrdering(lastResultPath, "PageRank/result");

        if (!isfinish) return 1;
        return 0;

}

   // function for Parsing 
public boolean ruFParsing(String[] args) throws IOException, ClassNotFoundException, InterruptedException
{

Configuration conf= new Configuration();
conf.setInt("counting",N);
 Job job = Job.getInstance(conf, " pageranking "); //creating job for page rank
        job.setJarByClass(this.getClass()); 
    FileInputFormat.addInputPaths(job, args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(MapperJob1.class);   //setting up corresponding mapper class for parsing the input file
    job.setReducerClass(ReducerJob1.class);  ////setting up corresponding reducer class
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true);
}
// Function for Order ranks of the titles 
 private boolean RankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
      
      Job rankOrder = Job.getInstance(getConf(), " pagerank ");// creating job
      rankOrder.setJarByClass(PageRank.class);
      rankOrder.setMapperClass(RankMap.class); // mapper class for rank ordering
      rankOrder.setReducerClass(RankReduce.class); //reducer class for rank ordering
      rankOrder.setMapOutputKeyClass(DoubleWritable.class);
      rankOrder.setMapOutputValueClass(Text.class);
      rankOrder.setSortComparatorClass(KeyComparator.class); // to sort the final output according to descending order of pagerank
      rankOrder.setOutputKeyClass(Text.class);
      rankOrder.setOutputValueClass(DoubleWritable.class);
      FileInputFormat.setInputPaths(rankOrder, new Path(inputPath));
      FileOutputFormat.setOutputPath(rankOrder, new Path(outputPath));
      rankOrder.setInputFormatClass(TextInputFormat.class);
      rankOrder.setOutputFormatClass(TextOutputFormat.class);

    return rankOrder.waitForCompletion(true);
    }

    // MykeyComparator Class for sorting the reducer output in descending order of pagerank

public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
          super(DoubleWritable.class,true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        DoubleWritable k1 = (DoubleWritable) o1;
        DoubleWritable k2 = (DoubleWritable) o2;         
        return -1 * k1.compareTo(k2);
    }
}

// job to calculate length/number of the files in the corpus

public boolean NumberOfNodes(String[]args) throws IOException, ClassNotFoundException, InterruptedException
{
Configuration conf= new Configuration();
Job job = Job.getInstance(conf, " pagerank ");
job.setJarByClass(this.getClass());
FileInputFormat.addInputPaths(job, args[0]);
FileOutputFormat.setOutputPath(job,new Path("PageRank/Nvalue"));// store Number of node dynamically 
job.setMapperClass(NumMap.class);
    job.setReducerClass(NumReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    return job.waitForCompletion(true);


}

//NumberMapper Class for finding number of input files in the corpus
public static class NumMap extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {

      private final static IntWritable one  = new IntWritable(1); //
      // word boundary for pattern matching
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*"); // a regular expression to parse each line of input text on word boundaries 

        public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString(); // to convert Text object to string
      
         if(!line.isEmpty())

            context.write(new Text("word"),one);
         }
      }
   
// end of NumMapper Class

// NumberReduceClass to count the total number of input files
public static class NumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
          int sum = 0;
            //get the count for each line and sum it up to get the total no of occurances
            for (IntWritable count : counts) {
                sum += count.get();
            }
           N=sum;  //assigning total number of files to global variable N
            // write the line along with number of occurances
            context.write(word, new IntWritable(sum));

}
}

//Hadoop Job 1 - Graph Link Structure

public static class MapperJob1 extends Mapper<LongWritable, Text, Text, Text> {

         
private static final Pattern link_word = Pattern .compile("\\[\\[.*?]\\]");


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       
        // Returns  String[0] = <title>[[TITLE]]</title>
        //          String[1] = <text>[[CONTENT]]</text>
       String[] titleandText = parseTitndtex(value);
        String pageString = titleandText[0];
        Text page = new Text(pageString);
        Matcher matcher = link_word.matcher(titleandText[1]);
       
        //Loop through the matched links in [[CONTENT]]
        while (matcher.find()) {// loop on each outgoing link
            String otherPage = matcher.group();
     // String url = m.group().replace("[[", "").replace("]]", ""); // drop the brackets and any nested ones if any
            //Filter only wiki pages
         // Do what ever you need with url, it now holds the title of the outgoing link.
            otherPage = getWikiPageFromLink(otherPage);
            if(otherPage == null || otherPage.isEmpty())
                continue;
           
            // add valid otheroutlinkedPages to the map.
            context.write(page, new Text(otherPage));
        }
    }

  private String getWikiPageFromLink(String wikilink){
     
       
        int start = wikilink.startsWith("[[") ? 2 : 1;
        int end = wikilink.indexOf("]");
        wikilink=wikilink.substring(start,end);
        return wikilink;
}
// prasing title and text 
private String[] parseTitndtex (Text value) throws CharacterCodingException {
        String[] titleandText = new String[2];
       //parsing title-wiki page 
        int start = value.find("<title>");
   
        int end = value.find("</title>");
   
       start += 7; //add <title> length.
       if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
       
        titleandText[0] = Text.decode(value.getBytes(), start, end-start);

       //parsing content for the wiki page
        start = value.find("<text>");
      
        end = value.find("</text>");
        start += 6;
       
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
       
        titleandText[1] = Text.decode(value.getBytes(), start, end-start);
       
        return titleandText;
    }
}

//reducer class stores the page with initial pagerank and outgoing links
public static class ReducerJob1 extends Reducer<Text, Text, Text, Text> {

 @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=context.getConfiguration().getInt("count",1);
        double ini_pagerank=1.0/(double)count; // assuming the initial page rank be (1/N)
        String pagerank = String.valueOf(ini_pagerank)+"\t";

        boolean first = true;

        for (Text value : values) {
            if(!first) pagerank += "#####";// diving with #####

            pagerank += value.toString();
            first = false;
        }
           context.write(key, new Text(pagerank));
    }
}

//Job 2: Calculate new page rank
public static class RankCalMap extends Mapper<LongWritable, Text, Text, Text> {
 

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int page_Tab_Index = value.find("\t");
        int rank_Tab_Index = value.find("\t", page_Tab_Index+1);

        String page = Text.decode(value.getBytes(), 0, page_Tab_Index);
        String pageWithRank = Text.decode(value.getBytes(), 0, rank_Tab_Index+1);
       
        // Mark page as an Existing page
        context.write(new Text(page), new Text("%"));  //(%) mark to indicate actual wiki page, reducer uses these pages to generate input

        // Skip pages with no links.
        if(rank_Tab_Index == -1) return;
       
        String links = Text.decode(value.getBytes(), rank_Tab_Index+1, value.getLength()-(rank_Tab_Index+1));
        String[] allOthPages = links.split("#####");// splitting using ##### and using for calculation
        int totalLinks = allOthPages.length;
       
        for (String otherPage : allOthPages){
            Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
            context.write(new Text(otherPage), pageRankTotalLinks);
        }
       
        // Put the original links of the page for the reduce output
        context.write(new Text(page), new Text("|" + links));
    }
}
//reducer calculates new pagerank and write it to the output for existing pages with the links on those pages
public static class RankCalReduce extends Reducer<Text, Text, Text, Text> {

private static final float dampingfactor = 0.85F; //damping factor=0.85

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isWikiPage = false;
        String[] str;
        float sumOtherPageRanks = 0;
        String links = "";
        String pageWithRank;
       
        // For each otherPage
        
        for (Text value : values){
            pageWithRank = value.toString();
           
            if(pageWithRank.equals("%")) {
                isWikiPage = true;
                continue;
            }
           
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }

            str = pageWithRank.split("\\t");
           
            float pageRank = Float.valueOf(str[1]);
            int countOutLinks = Integer.valueOf(str[2]); //number of outgoing links from the pages
           
            sumOtherPageRanks += (pageRank/countOutLinks);
        }

        if(!isWikiPage) return;
        float newRank = dampingfactor * sumOtherPageRanks + (1-dampingfactor); // pagerank formula to handle transporting and following link structure

        context.write(page, new Text(newRank + links));
    }
}
public static class RankMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pageAndRank = getPageAndRank(key, value);
       
        float parseFloat = Float.parseFloat(pageAndRank[1]);
      
        Text page = new Text(pageAndRank[0]);
       DoubleWritable rank = new DoubleWritable(parseFloat);

        context.write(rank, page);
    }
   //method to get page and its page rank and stored as String array
    private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
        
        String[] pageAndRank = new String[2];
        int page_index = value.find("\t");
        int rank_index = value.find("\t", page_index + 1);
        int last;
        if (rank_index == -1) {
            last = value.getLength() - (page_index + 1);
        } else {
            last= rank_index - (page_index + 1);
        }
       
        pageAndRank[0] = Text.decode(value.getBytes(), 0, page_index);
        pageAndRank[1] = Text.decode(value.getBytes(), page_index + 1, last);
       
        return pageAndRank;
    }
   }

   //rankingreduce job to produce the final output:Page<TAB>PageRank
    public static class RankReduce extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
      @Override
      public void reduce(DoubleWritable rank,  Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {

       for(Text wikipage:values)
       {
       context.write(wikipage,rank);
       }
}

}
}
