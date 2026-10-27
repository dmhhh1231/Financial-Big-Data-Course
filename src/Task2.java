
import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2 {

    // 分布式缓存文件在工作目录中的名称
    private static final String CACHE_NAME = "task1_stats";

    public enum DataCounter {
        MISSING_ORG,
        INVALID_AMOUNT,
        INVALID_ROW,
        MISSING_TASK1_STATS
    }

    // 保存任务1统计出的机构客户数量和资产总额
    private static class OrgStats {
        long count;
        BigDecimal totalAsset;

        OrgStats(long count, BigDecimal totalAsset) {
            this.count = count;
            this.totalAsset = totalAsset;
        }
    }

    // 解析CSV，支持空字段和双引号
    private static List<String> parseCsv(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {
                    field.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                result.add(field.toString().trim());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }

        result.add(field.toString().trim());
        return result;
    }

    // 根据表头自动确定字段位置
    private static int findColumn(List<String> header, String name)
            throws IOException {

        for (int i = 0; i < header.size(); i++) {
            String column = header.get(i)
                    .replace("\uFEFF", "").trim();

            if (column.equalsIgnoreCase(name)) {
                return i;
            }
        }

        throw new IOException("CSV缺少字段: " + name);
    }

    // 与Task1保持一致：缺失金额视为0
    private static BigDecimal parseAmount(String value) {
        String s = value.trim();

        if (s.isEmpty()
                || s.equalsIgnoreCase("null")
                || s.equalsIgnoreCase("nan")
                || s.equalsIgnoreCase("na")
                || s.equals("\\N")) {
            return BigDecimal.ZERO;
        }

        return new BigDecimal(s);
    }

    
    // 1. Mapper
    public static class Task2Mapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private int orgIndex;
        private int assetIndex;
        private int debtIndex;
        private int labelIndex;
        private int maxIndex;

        // 每个机构对应的Task1统计结果
        private final Map<String, OrgStats> orgStats =
                new HashMap<>();

        private final Text outKey = new Text();

        private static final Text HIGH = new Text("1,0");
        private static final Text LOW = new Text("0,1");

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            // 读取分布式缓存中的Task1结果
            try (BufferedReader reader = Files.newBufferedReader(
                    Paths.get(CACHE_NAME),
                    StandardCharsets.UTF_8)) {

                String line;

                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    // Task1输出:
                    // 机构编号 TAB 客户数,总资产,总负债,逾期数
                    String[] parts = line.split("\\t", 2);

                    if (parts.length != 2) {
                        throw new IOException(
                                "Task1结果格式错误: " + line);
                    }

                    String org = parts[0].trim();
                    String[] values = parts[1].split(",", -1);

                    if (values.length != 4) {
                        throw new IOException(
                                "Task1统计字段数量错误: " + line);
                    }

                    try {
                        long count =
                                Long.parseLong(values[0].trim());

                        BigDecimal totalAsset =
                                new BigDecimal(values[1].trim());

                        if (count <= 0) {
                            throw new IOException(
                                    "机构客户数量必须大于0: " + org);
                        }

                        orgStats.put(
                                org,
                                new OrgStats(count, totalAsset)
                        );

                    } catch (NumberFormatException e) {
                        throw new IOException(
                                "Task1结果数字解析失败: " + line, e);
                    }
                }
            }

            if (orgStats.isEmpty()) {
                throw new IOException(
                        "Task1统计结果为空，无法计算平均值");
            }

            // 读取train.csv的表头
            FileSplit split =
                    (FileSplit) context.getInputSplit();

            Path inputFile = split.getPath();

            try (BufferedReader reader = new BufferedReader(
                    new java.io.InputStreamReader(
                            inputFile.getFileSystem(
                                    context.getConfiguration())
                                    .open(inputFile),
                            StandardCharsets.UTF_8))) {

                String firstLine = reader.readLine();

                if (firstLine == null) {
                    throw new IOException("train.csv为空");
                }

                List<String> header = parseCsv(firstLine);

                orgIndex =
                        findColumn(header, "OPEN_ORG_NUM");
                assetIndex =
                        findColumn(header, "CUST_ASSET_AMT");
                debtIndex =
                        findColumn(header, "CUST_DEBT_AMT");
                labelIndex =
                        findColumn(header, "bad_good");

                maxIndex = Math.max(
                        Math.max(orgIndex, assetIndex),
                        Math.max(debtIndex, labelIndex)
                );
            }
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 跳过CSV表头和空行
            if (key.get() == 0 || line.trim().isEmpty()) {
                return;
            }

            List<String> fields = parseCsv(line);

            if (fields.size() <= maxIndex) {
                context.getCounter(
                        DataCounter.INVALID_ROW).increment(1);
                return;
            }

            String org = fields.get(orgIndex).trim();

            if (org.isEmpty()) {
                context.getCounter(
                        DataCounter.MISSING_ORG).increment(1);
                return;
            }

            BigDecimal asset;

            try {
                asset = parseAmount(fields.get(assetIndex));

                // Task1也检查负债字段。
                // 此处保持有效记录的筛选规则一致。
                parseAmount(fields.get(debtIndex));

            } catch (NumberFormatException e) {
                context.getCounter(
                        DataCounter.INVALID_AMOUNT).increment(1);
                return;
            }

            OrgStats stats = orgStats.get(org);

            if (stats == null) {
                context.getCounter(
                        DataCounter.MISSING_TASK1_STATS)
                        .increment(1);
                return;
            }

            // 平均资产 = 总资产 / 客户数量
            //
            // 不直接进行除法，而是比较：
            // 当前资产 * 客户数量 >= 总资产
            //
            // 避免除法产生循环小数或精度误差。
            BigDecimal left = asset.multiply(
                    BigDecimal.valueOf(stats.count));

            int comparison = left.compareTo(stats.totalAsset);

            outKey.set(org);

            if (comparison >= 0) {
                // 资产大于或等于机构平均值
                context.write(outKey, HIGH);
            } else {
                // 资产低于机构平均值
                context.write(outKey, LOW);
            }
        }
    }

    // 2. Combiner和Reducer
    public static class Task2Reducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key,
                              Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            long highCount = 0;
            long lowCount = 0;

            for (Text value : values) {
                String[] parts =
                        value.toString().split(",", -1);

                if (parts.length != 2) {
                    throw new IOException(
                            "中间结果格式错误: " + value);
                }

                try {
                    highCount += Long.parseLong(parts[0]);
                    lowCount += Long.parseLong(parts[1]);
                } catch (NumberFormatException e) {
                    throw new IOException(
                            "中间结果解析失败: " + value, e);
                }
            }

            // 输出: 机构编号 TAB 高于等于平均值人数,低于平均值人数
            context.write(
                    key,
                    new Text(highCount + "," + lowCount)
            );
        }
    }

    // 3. Driver
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "用法: Task2 <train.csv> "
                    + "<Task1结果文件> <Task2输出目录>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf, "Task2 - Customer Asset Average Analysis");

        job.setJarByClass(Task2.class);

        // Mapper
        job.setMapperClass(Task2Mapper.class);

        // Combiner：本地汇总计数
        job.setCombinerClass(Task2Reducer.class);

        // Reducer：最终汇总
        job.setReducerClass(Task2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 使用一个Reducer，生成单个结果文件
        job.setNumReduceTasks(1);

        // 第一个参数：train.csv在HDFS中的路径
        FileInputFormat.addInputPath(
                job, new Path(args[0]));

        // 第二个参数：Task1的输出文件
        Path task1File = new Path(args[1]);
        FileSystem fs = task1File.getFileSystem(conf);

        if (!fs.isFile(task1File)) {
            throw new IOException(
                    "找不到Task1结果文件: " + args[1]);
        }

        // 使用完整HDFS URI加入分布式缓存
        Path qualifiedPath = fs.makeQualified(task1File);

        URI cacheUri = URI.create(
                qualifiedPath.toUri().toString()
                        + "#" + CACHE_NAME);

        job.addCacheFile(cacheUri);

        // 第三个参数：Task2输出目录
        FileOutputFormat.setOutputPath(
                job, new Path(args[2]));

        // 提交并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}