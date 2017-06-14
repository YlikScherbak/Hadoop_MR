package movie;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text year = new Text();
    private Text genres = new Text();
    private static Pattern patternYear = Pattern.compile("\\(\\d{4}\\)");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (!line.isEmpty() && line.length() > 10) {
            String[] parts = line.split(",");
            Matcher matcher = patternYear.matcher(line);
            if (matcher.find()) {
                year.set(matcher.group());
            } else return;


            try {
                for (String genre : getGenres(parts[parts.length - 1])) {
                    genres.set(genre);
                    context.write(year, genres);
                }
            } catch (NullPointerException e) {//RIP
            }
        }

    }

    public String[] getGenres(String s) {
        if (s.contains("|")) {
            String[] genres = s.split("\\|");
            genres[genres.length - 1] = trimWord(genres[genres.length - 1]);
            return genres;
        }else if (!s.contains("|") && !s.trim().equals("(no genres listed)")){
            return new String[]{trimWord(s)};
        }
        return new String[]{};
    }

    public String trimWord(String str){
        if (str.contains("\"")){
            str = str.replace("\"", "").trim();
            return str;
        }else return str.trim();
    }

}
