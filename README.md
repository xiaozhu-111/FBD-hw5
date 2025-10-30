# FBD-hw5
this part include my homework5 of Financial Big Data Processing Technology. Mainly about sentiment analysis count.

---

# 设计思路
首先观察csv文件，注意到其中有一部分乱码（例如 <font style="color:rgb(15, 17, 21);background-color:rgb(237, 243, 254);">â€¦</font> 等），有一部分text包含双引号，并且还有一些网站的URL，前后两个字段用逗号分离，双引号内的逗号在分离字段的时候不考虑。因此我的设计思路如下：

## 第一阶段：<font style="color:rgb(15, 17, 21);">词汇计数统计</font>
### <font style="color:rgb(15, 17, 21);">csv解析（Mapper）</font>
<font style="color:rgb(15, 17, 21);">使用自定义CSV解析器处理可能包含引号和逗号的文本，将引号外的逗号视为字段分隔符，引号内的逗号仅视作某一个字段内的普通标点符号后续处理</font>

### <font style="color:rgb(15, 17, 21);">文本预处理（Mapper）</font>
首先清理文本，将原始文本进行如下处理：

所有字母转小写 → 去除除了字母以外的字符（例如标点符号等） →  移除数字  →  去除首尾空格  → 根据空格划分单词 → 停用词过滤，避免停用词计数（将https划分到停用词的类别中不在结果中进行计数），长度小于等于一的部分不计数（例如单个字母和空字符串）

### 按照键值对计数（Reduce）
将每个单词作为键，将计数作为值，记录所有单词分别共计出现了多少次

## 第二阶段：排序并分别取Top100
### **<font style="color:rgb(15, 17, 21);">Mapper设计思路</font>**

数据重组：将第一阶段的输出重新组织分类
例如：
**input：positive_word\t3**
**output：positive → apple\t3**
目的：按情感类别将处理好的单词结果重新进行分组，便于Reducer分别排序

### **<font style="color:rgb(15, 17, 21);">Reducer设计思路</font>**
#### **<font style="color:rgb(15, 17, 21);">排序策略</font>**
<font style="color:rgb(15, 17, 21);">使用TreeMap实现自动排序</font>

<font style="color:rgb(15, 17, 21);">优势：</font>

+ <font style="color:rgb(15, 17, 21);">TreeMap自动按键排序</font>
+ <font style="color:rgb(15, 17, 21);">Collections.reverseOrder()实现降序排列</font>
+ <font style="color:rgb(15, 17, 21);">支持同频词处理（List<String>存储同频词）</font>

#### **<font style="color:rgb(15, 17, 21);">TopN输出</font>**
直接遍历刚才的TreeMap，遍历过程中由大到小输出

# **伪代码**
```java
# 第一阶段：词频统计
FOR each line in 输入数据:
    IF line是表头 THEN CONTINUE
    devide CSV and get：text和sentiment
    IF sentiment不是1或-1 THEN CONTINUE
    
    # 文本清洗
    cleaned_text = 转小写(text)
               .移除非字母字符()
               .移除数字()
               .去空格()
    
    # 分词和过滤
    FOR EACH word IN 分词(cleaned_text):
        IF word是"https" OR word长度≤1 OR word在停用词表 THEN CONTINUE
        
        IF sentiment == 1:
            record as("positive_" + word, 1)//1表示数量（键值对的存储方式）
        ELSE:
            record as("negative_" + word, 1)

# 词频汇总
FOR EACH key IN 所有输出键:
    total = SUM(所有对应值)
    record as(key, total)

# 第二阶段：排序取Top100
FOR EACH 词频记录 IN 第一阶段输出:
    IF key以"positive_"开头:
        turn into("positive", 单词 + "\t" + 词频)
    ELSE IF key以"negative_"开头:
        turn into("negative", 单词 + "\t" + 词频)

# 对每个情感类别排序
FOR EACH sentiment IN ["positive", "negative"]:
    创建词频映射表(按词频降序排序)
    
    初始化cnt为0
    FOR EACH 词频 FROM 高到低:
        FOR EACH 该词频的所有单词:
            IF emitted < 100:
                EMIT(单词, 词频)
                cnt++
            ELSE:
                BREAK
        IF cnt >= 100: BREAK
```

# 代码具体实现
## 第一阶段：<font style="color:rgb(15, 17, 21);">词汇计数统计</font>
### <font style="color:rgb(15, 17, 21);">csv解析</font>
```plain
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
```

### <font style="color:rgb(15, 17, 21);">文本预处理（Mapper）</font>
```java
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
                            //直接作为停用词删掉
                            continue; // 跳过下面的长度/停用词检查
                        }
                          // 过滤停用词、长度≤1的词以及空字符串
                        if (cleanWord.length() > 1 && !stopwords.contains(cleanWord) && !cleanWord.isEmpty()) {
                            String outputKey = (sentiment == 1 ? "positive_" : "negative_") + cleanWord;
                            word.set(outputKey);
                            //输出键值对，值用来计数，键是情感前缀+文本
                            context.write(word, one);
                        }
                    }
```

### 按照键值对计数（Reduce）
```java
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
```

## 第二阶段：排序并分别取Top100
### **<font style="color:rgb(15, 17, 21);">Mapper设计思路</font>**
```java
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

```

### **<font style="color:rgb(15, 17, 21);">Reducer设计思路</font>**
#### 排序
```java
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
```

#### TopN输出
```java
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
```

# 运行操作
## <font style="color:rgb(15, 17, 21);">在HDFS上创建目录结构并将文件导入</font>
`hadoop fs -mkdir -p /user/zxz/stock_data/input`

`hadoop fs -mkdir -p /user/zxz/stock_data/stopwords`

`hadoop fs -put /home/mininet/stock_data.csv /user/zxz/stock_data/input/`

`hadoop fs -put /home/mininet/stop-word-list.txt /user/zxz/stock_data/stopwords/`

**hdfs分布式文件系统与实验相关目录结构：**

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761202302954-ea23bb83-697e-46de-8409-eb85ce257ec8.png)

## 命令行运行口令
```plain
# 创建目录
mkdir -p stock_classes

//编译
mininet@mininet-vm:~/Finance_Data_Tech/hw5/StockSentimentAnalysis$ javac -classpath `hadoop classpath` -d stock_classes src/StockSentimentAnalysis.java

//打包class文件夹
mininet@mininet-vm:~/Finance_Data_Tech/hw5/StockSentimentAnalysis$ jar -cvf stock_sentiment.jar -C stock_classes .

//运行Hadoop文件
mininet@mininet-vm:~/Finance_Data_Tech/hw5/StockSentimentAnalysis$ hadoop jar stock_sentiment.jar StockSentimentAnalysis /user/zxz/stock_data/input /user/zxz/stock_data/final_output
```

目录结构如下

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761565788672-89e5315d-a624-400a-aef2-eb1181108d5c.png)![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761574562833-e33d2461-7a65-4a73-8043-9af04b484636.png)

## 命令行Hadoop运行结果
可以看到两个部分的job都运行成功了

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761578871785-b9a7e91e-b9a3-4988-b421-37e1ae8f9d23.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761578892810-f16d07ee-9e10-42fa-86fe-971e742298ad.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761578905991-4f419d9a-9074-40ef-abfb-3b66b69687eb.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761578920582-e322eda4-fec1-4099-ad76-082f131e95de.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761578930206-ff96def2-08ad-4645-9caf-215c46918edd.png)

## Web界面查看运行结果
可以看到我两个job模块均成功了（因为前边不断尝试不间断的跑了太多次了，导致越往后我的虚拟机运行的越慢了）

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761578846591-67c57428-c132-4e38-98c3-affc24176270.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1761578802315-c71dbfbc-523b-4af0-8817-144bb7ac3177.png)

## 把文件下载到本地查看并拆分
```plain
//把包含全部数据的文件下载下来
mininet@mininet-vm:~/Finance_Data_Tech/hw5/StockSentimentAnalysis$ hadoop fs -get /user/zxz/stock_data/final_output/
part-r-00000 ./all_results.txt

//拆分出全部文件中正向情感的100个
mininet@mininet-vm:~/Finance_Data_Tech/hw5/StockSentimentAnalysis$ cat ./all_results.txt | head -100 > negative_top1
00.txt

//拆分出全部文件中负向情感的100个
mininet@mininet-vm:~/Finance_Data_Tech/hw5/StockSentimentAnalysis$ cat ./all_results.txt | tail -100 > positive_top1
00.txt
```

# 性能、扩展性的不足和可改进方面分析
+ 文本清洗有点暴力：移除了所有数字以及金融符号、URL被直接暴力拆解，很可能丢失股票代码或者网页链接等信息，后续可以根据选择保留有可能需要的东西，例如URL可以单独处理不进行拆解
+ 添加中间结果验证：可以中间增加一些调试输出，这样更方面看到哪里出问题
+ 可以优化一下输出格式，我现在的逻辑第一步输出的时候格式是positive_word或者negative_word

# 完整的全部代码
```java
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
```


