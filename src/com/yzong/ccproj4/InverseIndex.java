package com.yzong.ccproj4;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class InverseIndex {

  static String StopWords[] = new String[] {"a", "a's", "able", "about", "above", "according",
      "accordingly", "across", "actually", "after", "afterwards", "again", "against", "ain't",
      "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although",
      "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow",
      "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate",
      "appropriate", "are", "aren't", "around", "as", "aside", "ask", "asking", "associated", "at",
      "available", "away", "awfully", "b", "be", "became", "because", "become", "becomes",
      "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside",
      "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "c", "c'mon",
      "c's", "came", "can", "can't", "cannot", "cant", "cause", "causes", "certain", "certainly",
      "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider",
      "considering", "contain", "containing", "contains", "corresponding", "could", "couldn't",
      "course", "currently", "d", "definitely", "described", "despite", "did", "didn't",
      "different", "do", "does", "doesn't", "doing", "don't", "done", "down", "downwards",
      "during", "e", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough",
      "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone",
      "everything", "everywhere", "ex", "exactly", "example", "except", "f", "far", "few", "fifth",
      "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth",
      "four", "from", "further", "furthermore", "g", "get", "gets", "getting", "given", "gives",
      "go", "goes", "going", "gone", "got", "gotten", "greetings", "h", "had", "hadn't", "happens",
      "hardly", "has", "hasn't", "have", "haven't", "having", "he", "he's", "hello", "help",
      "hence", "her", "here", "here's", "hereafter", "hereby", "herein", "hereupon", "hers",
      "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however",
      "i", "i'd", "i'll", "i'm", "i've", "ie", "if", "ignored", "immediate", "in", "inasmuch",
      "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into",
      "inward", "is", "isn't", "it", "it'd", "it'll", "it's", "its", "itself", "j", "just", "k",
      "keep", "keeps", "kept", "know", "knows", "known", "l", "last", "lately", "later", "latter",
      "latterly", "least", "less", "lest", "let", "let's", "like", "liked", "likely", "little",
      "look", "looking", "looks", "ltd", "m", "mainly", "many", "may", "maybe", "me", "mean",
      "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my",
      "myself", "n", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs",
      "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none",
      "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "o", "obviously",
      "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto",
      "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside",
      "over", "overall", "own", "p", "particular", "particularly", "per", "perhaps", "placed",
      "please", "plus", "possible", "presumably", "probably", "provides", "q", "que", "quite",
      "qv", "r", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless",
      "regards", "relatively", "respectively", "right", "s", "said", "same", "saw", "say",
      "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming",
      "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven",
      "several", "shall", "she", "should", "shouldn't", "since", "six", "so", "some", "somebody",
      "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon",
      "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "t",
      "t's", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that",
      "that's", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there",
      "there's", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these",
      "they", "they'd", "they'll", "they're", "they've", "think", "third", "this", "thorough",
      "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to",
      "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying",
      "twice", "two", "u", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto",
      "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "uucp", "v",
      "value", "various", "very", "via", "viz", "vs", "w", "want", "wants", "was", "wasn't", "way",
      "we", "we'd", "we'll", "we're", "we've", "welcome", "well", "went", "were", "weren't",
      "what", "what's", "whatever", "when", "whence", "whenever", "where", "where's", "whereafter",
      "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while",
      "whither", "who", "who's", "whoever", "whole", "whom", "whose", "why", "will", "willing",
      "wish", "with", "within", "without", "won't", "wonder", "would", "would", "wouldn't", "x",
      "y", "yes", "yet", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
      "yourselves", "z", "zero"};

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      Text filename = new Text(((FileSplit) reporter.getInputSplit()).getPath().getName());
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        // Skip if stopper word.
        if (Arrays.binarySearch(StopWords, token) >= 0) {
          continue;
        }
        word.set(token);
        output.collect(word, filename);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      String prevFile = values.next().toString();
      String fileList = prevFile + ",";
      while (values.hasNext()) {
        String currFile = values.next().toString();
        // Only include the file name if different from previous one.
        if (currFile.equals(prevFile) == false) {
          fileList += currFile + ",";
          prevFile = currFile;
        }
      }
      fileList = fileList.substring(0, fileList.length() - 1);
      output.collect(key, new Text(fileList));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(InverseIndex.class);
    conf.setJobName("inverseindex");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setNumMapTasks(10);
    conf.setNumReduceTasks(10);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
