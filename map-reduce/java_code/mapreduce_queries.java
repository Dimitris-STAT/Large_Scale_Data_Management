package gr.aueb.panagiotisl.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

import javax.naming.Context;

public class mapreduce_queries {
    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
        // declare outputKey and outputValue as class members
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();
        
        /**
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            // ignore the header(first line)
            if (key.get() == 0){ // if line number(index) == 0 pass
                return;
            }
            // split a line into words
            String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            
    
            // variables for storing values
            String song_name = null;
            String country = null;
            String date = null;
            String dance = null;

            // extract value from the string "value"
            tokens[6] = tokens[6].replaceAll("\"", "");
            // check first if the word is missing from 6th position -> country
            if(tokens[6] != null && !tokens[6].isEmpty()){
                // output (word, 1)
                // run down all the array(tokens) and hold only those string values that
                // are in the positions 1, 6, 7, 13 referencing back to the original csv distribution of columns
                for (int i = 0; i < tokens.length; i++) {
                    // here we cut down the not needed columns based on position on the array
                    // this happens for each line of the csv at a time!
                    
                        // for the song_name
                        if (i == 1){song_name = tokens[i];} 
                        // for example (word(country), 6)}
                        if( i == 6 ) {country = tokens[i];}
                        // get the date from the csv
                        if (i == 7){date = tokens[i];} 
                        // get the danceability number from the csv
                        // import it as string 
                        if (i == 13){dance = tokens[i];}                    
                    }
                // i.e. derive the format 2020-01 from 2020-01-10
                date = date.substring(1, 8);
                // build the keys 
                outputKey.set(new Text(country + ":" + date));
                // build the output values
                outputValue.set(new Text(song_name + "," + dance));
                // build the list that the reducer will utilize
                context.write(outputKey, outputValue);
            }
            // else pass the current line (country = empty-string)
            else{return;}
        }
    }

    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
         // declare outputKey and outputValue as class members
         private final Text outputKey = new Text();
         private final Text outputValue = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int total = 0;

            // variables for storing values
            String song_name = null;
            Float dance = null;
            
            // variables for storing values
            String mostDanceableSong = null;
            float maxDanceability = Float.MIN_VALUE; // very small float value 

            // variable to track whether the most danceable song has been found
            Boolean mostDanceableFound = null;


            // iterate throw the list values
            for (Text value: values){
                // separate the values by commas as the input format suggests
                String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            
                // parse the dance value to float and increment the sum
                // but first remove any unnecessary quotes inside the tokens[1]
                float danceValue = Float.parseFloat(tokens[1].replaceAll("\"", ""));
                song_name = tokens[0];
                sum += danceValue;
                total++; // increment the total count

                // update most danceable song if current song is more danceable
                if (danceValue > maxDanceability) {
                    mostDanceableSong = song_name;
                    maxDanceability = danceValue;
                }
            }
            // emit output only if the most danceable song has been found
            
            // calculate average only if there are valid values
            float avg = (total > 0) ? sum / total : 0;
            
            // build the ouputvalue for average and max danceability
            outputValue.set(new Text(mostDanceableSong + ": " + maxDanceability + ", avg: " + avg));

            // output (country-date pair, outputValue)
            context.write(key, outputValue);  
        }
    }
}
