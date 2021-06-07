import java.io.IOException;
import java.util.StringTokenizer;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataDistribution {
  public static final String england = "england";
  public static final String spain = "spain";
  public static final String italy = "italy";
  public static final String france = "france";
  public static final String germany = "germany";
  public static final String season_2012 ="2012";
  public static final String season_2013 ="2013";
  public static final String season_2014 ="2014";
  public static final String season_2015 ="2015";
  public static final String season_2016 ="2016";
  public static final String season_2017 ="2017";
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    //MultipleOutputs multipleOutputs;
    // @Override
    // protected void setup(Context context) throws IOException, InterruptedException {
    //     multipleOutputs = new MultipleOutputs(context);
    // }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //MultipleOutputs multipleOutputs= new MultipleOutputs(context);
      String preprocessed_value = value.toString();           
      preprocessed_value = preprocessed_value.replace("\"", "");
      preprocessed_value = preprocessed_value.replace(" ", "-");
      StringTokenizer itr = new StringTokenizer(preprocessed_value);
      while (itr.hasMoreTokens()) {
        String [] temp=itr.nextToken().split(",");
        String string_value = String.join(",", temp);
        word.set(temp[6]);// Choose the country as the key
        context.write(word, new Text(string_value));
      }
    }

    // @Override
    // protected void cleanup(Context context) throws IOException, InterruptedException {
    //     multipleOutputs.close();
    // }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    MultipleOutputs multipleOutputs;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // write file for each key                    
      String value_results="";                   
      for (Text val : values) {
        //value_results += val.toString()+'\n';
        result.set(val.toString());
        String [] temp=val.toString().split(",");
        if(key.toString().equals(england)){
          if(temp[5].equals(season_2012)){
            multipleOutputs.write(england, key,  result, "england/england_2012");
          }else if (temp[5].equals(season_2013)){
            multipleOutputs.write(england, key,  result, "england/england_2013");
          }else if (temp[5].equals(season_2014)){
            multipleOutputs.write(england, key,  result, "england/england_2014");
          }else if (temp[5].equals(season_2015)){
            multipleOutputs.write(england, key,  result, "england/england_2015");
          }else if (temp[5].equals(season_2016)){
            multipleOutputs.write(england, key,  result, "england/england_2016");
          }else if (temp[5].equals(season_2017)){
            multipleOutputs.write(england, key,  result, "england/england_2017");
          }
        }else if (key.toString().equals(spain)){
          if(temp[5].equals(season_2012)){
            multipleOutputs.write(england, key,  result, "spain/spain_2012");
          }else if (temp[5].equals(season_2013)){
            multipleOutputs.write(england, key,  result, "spain/spain_2013");
          }else if (temp[5].equals(season_2014)){
            multipleOutputs.write(england, key,  result, "spain/spain_2014");
          }else if (temp[5].equals(season_2015)){
            multipleOutputs.write(england, key,  result, "spain/spain_2015");
          }else if (temp[5].equals(season_2016)){
            multipleOutputs.write(england, key,  result, "spain/spain_2016");
          }else if (temp[5].equals(season_2017)){
            multipleOutputs.write(england, key,  result, "spain/spain_2017");
          }
        }else if (key.toString().equals(italy)){
          if(temp[5].equals(season_2012)){
            multipleOutputs.write(england, key,  result, "italy/italy_2012");
          }else if (temp[5].equals(season_2013)){
            multipleOutputs.write(england, key,  result, "italy/italy_2013");
          }else if (temp[5].equals(season_2014)){
            multipleOutputs.write(england, key,  result, "italy/italy_2014");
          }else if (temp[5].equals(season_2015)){
            multipleOutputs.write(england, key,  result, "italy/italy_2015");
          }else if (temp[5].equals(season_2016)){
            multipleOutputs.write(england, key,  result, "italy/italy_2016");
          }else if (temp[5].equals(season_2017)){
            multipleOutputs.write(england, key,  result, "italy/italy_2017");
          }
        }else if (key.toString().equals(france)){
          if(temp[5].equals(season_2012)){
            multipleOutputs.write(england, key,  result, "france/france_2012");
          }else if (temp[5].equals(season_2013)){
            multipleOutputs.write(england, key,  result, "france/france_2013");
          }else if (temp[5].equals(season_2014)){
            multipleOutputs.write(england, key,  result, "france/france_2014");
          }else if (temp[5].equals(season_2015)){
            multipleOutputs.write(england, key,  result, "france/france_2015");
          }else if (temp[5].equals(season_2016)){
            multipleOutputs.write(england, key,  result, "france/france_2016");
          }else if (temp[5].equals(season_2017)){
            multipleOutputs.write(england, key,  result, "france/france_2017");
          }
        }else{
          if(temp[5].equals(season_2012)){
            multipleOutputs.write(england, key,  result, "germany/germany_2012");
          }else if (temp[5].equals(season_2013)){
            multipleOutputs.write(england, key,  result, "germany/germany_2013");
          }else if (temp[5].equals(season_2014)){
            multipleOutputs.write(england, key,  result, "germany/germany_2014");
          }else if (temp[5].equals(season_2015)){
            multipleOutputs.write(england, key,  result, "germany/germany_2015");
          }else if (temp[5].equals(season_2016)){
            multipleOutputs.write(england, key,  result, "germany/germany_2016");
          }else if (temp[5].equals(season_2017)){
            multipleOutputs.write(england, key,  result, "germany/germany_2017");
          }
        }
      }
      //result.set(value_results);
      //context.write(key, result);
      // if(key.toString().equals(england)){
      //   multipleOutputs.write(england, key,  result, "england/england");
      // }else if (key.toString().equals(spain)){
      //   multipleOutputs.write(spain, key,  result,"spain/spain");
      // }else if (key.toString().equals(italy)){
      //   multipleOutputs.write(italy, key,  result,"italy/italy");
      // }else if (key.toString().equals(france)){
      //   multipleOutputs.write(france, key,  result,"france/france");
      // }else{
      //   multipleOutputs.write(germany, key,  result,"germany/germany");
      // }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator",":");
    Job job = Job.getInstance(conf, "word count");
    job.setSpeculativeExecution(false);
    job.setJarByClass(DataDistribution.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    MultipleOutputs.addNamedOutput(job,"england", TextOutputFormat.class,Text.class,Text.class);
    MultipleOutputs.addNamedOutput(job,"spain", TextOutputFormat.class,Text.class,Text.class);
    MultipleOutputs.addNamedOutput(job,"italy", TextOutputFormat.class,Text.class,Text.class);
    MultipleOutputs.addNamedOutput(job,"france", TextOutputFormat.class,Text.class,Text.class);
    MultipleOutputs.addNamedOutput(job,"germany", TextOutputFormat.class,Text.class,Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}