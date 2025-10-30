import java.io.*;
// import java.nio.file.Path;
import org.apache.hadoop.fs.Path;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockSentimentAnalysis {

    // 第一个MapReduce：单词计数
    public static class TokenizerMapper 
         extends Mapper<Object, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopwords = new HashSet<>();
        
        @Override
        protected void setup(Context context) throws IOException {
            // 从分布式缓存读取停用词
            try {
                Path[] cacheFiles = context.getLocalCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    BufferedReader reader = new BufferedReader(
                        new FileReader(cacheFiles[0].toString()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stopwords.add(line.trim().toLowerCase());
                    }
                    reader.close();
                }
            } catch (Exception e) {
                System.err.println("Error reading stopwords: " + e.getMessage());
            }
        }
        
        // 简单的CSV解析方法
        private String[] parseCSVLine(String line) {
            //存储每个csv字段
            List<String> fields = new ArrayList<>();
            //累加当前字段的字符
            StringBuilder field = new StringBuilder();
            boolean inQuotes = false;//标记是否在双引号内，双引号内的逗号不视作字段分隔符
            
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                
                if (c == '"') {
                    inQuotes = !inQuotes;//如果遇到双引号则进入或离开引号
                } else if (c == ',' && !inQuotes){
                    //如果遇到逗号并且逗号不在引号内，说明一个字段结束了

                    //将当前字段加入列表，去掉首尾空格
                    fields.add(field.toString().trim());
                    //清空field准备迎接下一个字段
                    field.setLength(0);
                } else {
                    field.append(c);
                }
            }
            fields.add(field.toString().trim());
            
            return fields.toArray(new String[0]);
        }
        
        public void map(Object key, Text value, Context context
                       ) throws IOException, InterruptedException {
            
            String line = value.toString();
            
            // 跳过表头因为第一行是列名
            if (line.startsWith("Text,Sentiment")) {
                return;
            }
            
            try {
                // 解析CSV行，调用刚才的函数把csv解析成字段数组
                String[] fields = parseCSVLine(line);
                
                if (fields.length >= 2) {
                    String text = fields[0];
                    String sentimentStr = fields[1];
                    
                    // 处理引号
                    if (text.startsWith("\"") && text.endsWith("\"")) {
                        text = text.substring(1, text.length() - 1);
                    }
                    
                    int sentiment;
                    try {
                        //把情感标签字符串转换成整数
                        sentiment = Integer.parseInt(sentimentStr.trim());
                    } catch (NumberFormatException e) {
                        return; // 跳过无效的情感标签
                    }
                    
                    // 只处理正面(1)和负面(-1)情感
                    if (sentiment != 1 && sentiment != -1) {
                        System.err.println("有错误！！！！");
                        return;
                    }
                    
                    // 文本清洗：转小写、去标点、去数字
                    String cleanedText = text.toLowerCase()
                                        //   .replaceAll("(https?://\\S+)", " URLPLACEHOLDER ")
                                          .replaceAll("[^a-z\\s]", " ")
                                          .replaceAll("\\d+", "")
                                          .trim();
                    
                    // 分词处理
                    String[] words = cleanedText.split("\\s+");
                    
                    for (String w : words) {
                        String cleanWord = w.trim();
                        
                        if ("https".equals(cleanWord)) {
                            // String outputKey = (sentiment == 1 ? "positive_" : "negative_") + cleanWord;
                            // word.set(outputKey);
                            // context.write(word, one);
                            //直接作为停用词删掉
                            continue; // 跳过下面的长度/停用词检查
                        }
                          // 过滤停用词、长度≤1的词以及空字符串
                        if (cleanWord.length() > 1 && !stopwords.contains(cleanWord) && !cleanWord.isEmpty()) {
                            // if ("urlplaceholder".equals(cleanWord)) {
                            // cleanWord = "https";
                            // }
                            String outputKey = (sentiment == 1 ? "positive_" : "negative_") + cleanWord;
                            word.set(outputKey);
                            //输出键值对，值用来计数，键是情感前缀+文本
                            context.write(word, one);
                        }
                    }
                }
            } catch (Exception e) {
                // 跳过解析错误的行
                System.err.println("Error parsing line: " + line);
                e.printStackTrace();
            }
        }
    }
    
    // 第一个Reducer：计数汇总
    public static class IntSumReducer 
         extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, 
                          Context context
                         ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // 第二个MapReduce的Mapper：为排序准备数据
    public static class SortMapper 
         extends Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Context context
                       ) throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] parts = line.split("\\t");
            if (parts.length == 2) {
                String keyWord = parts[0]; // positive_word or negative_word
                String count = parts[1];
                
                // 提取情感类型和单词
                if (keyWord.startsWith("positive_")) {
                    String word = keyWord.substring(9);
                    context.write(new Text("positive"), new Text(word + "\t" + count));
                } else if (keyWord.startsWith("negative_")) {
                    String word = keyWord.substring(9);
                    context.write(new Text("negative"), new Text(word + "\t" + count));
                }
            }
        }
    }

    // 第二个MapReduce的Reducer：排序并取前100
    public static class TopNReducer 
         extends Reducer<Text, Text, Text, IntWritable> {
        
        private static final int N = 100;
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();
        
        public void reduce(Text key, Iterable<Text> values, 
                          Context context
                         ) throws IOException, InterruptedException {
            
            // 使用TreeMap自动按词频降序排序
            TreeMap<Integer, List<String>> countMap = new TreeMap<>(Collections.reverseOrder());
            
            for (Text val : values) {
                String[] parts = val.toString().split("\\t");
                if (parts.length == 2) {
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    
                    if (!countMap.containsKey(count)) {
                        countMap.put(count, new ArrayList<>());
                    }
                    countMap.get(count).add(word);
                }
            }
            
            // 输出前N个高频词
            int emitted = 0;
            for (Map.Entry<Integer, List<String>> entry : countMap.entrySet()) {
                int count = entry.getKey();
                for (String word : entry.getValue()) {
                    if (emitted < N) {
                        outputKey.set(word);
                        outputValue.set(count);
                        context.write(outputKey, outputValue);
                        emitted++;
                    } else {
                        break;
                    }
                }
                if (emitted >= N) break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StockSentimentAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        
        // 第一个Job：单词计数
        Job job1 = Job.getInstance(conf, "stock sentiment word count");
        job1.setJarByClass(StockSentimentAnalysis.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        // 添加停用词文件到分布式缓存
        job1.addCacheFile(new Path("/user/zxz/stock_data/stopwords/stop-word-list.txt").toUri());
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/user/zxz/stock_data/temp_output"));
        
        boolean success = job1.waitForCompletion(true);
        
        if (success) {
            // 第二个Job：排序和取前100
            Job job2 = Job.getInstance(conf, "top 100 words by sentiment");
            job2.setJarByClass(StockSentimentAnalysis.class);
            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(TopNReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job2, new Path("/user/zxz/stock_data/temp_output"));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}