
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task4 {

    // 分布式缓存中的Task3结果文件名称
    private static final String CACHE_NAME = "task3_quartiles";

    // 四个资产等级，按要求排列
    private static final String[] LEVEL_NAMES = {
        "低资产",
        "中等资产",
        "较高资产",
        "高资产"
    };

    // 七类资产对应的CSV字段
    private static final String[] ASSET_COLUMNS = {
        "CUST_SAVING_AMT",
        "CUST_FINA_AMT",
        "CUST_PRIFINA_AMT",
        "CUST_FOND_AMT",
        "CUST_NADEBT_AMT",
        "CUST_YBT_AMT",
        "CUST_METAL_AMT"
    };

    // 七类资产的中文名称
    private static final String[] ASSET_NAMES = {
        "存款",
        "理财",
        "私人理财",
        "基金",
        "国债",
        "银保通",
        "贵金属"
    };

    private static final int ASSET_COUNT = 7;

    // 记录数据异常情况
    public enum DataCounter {
        INVALID_ROW,
        INVALID_ASSET,
        INVALID_CATEGORY_AMOUNT,
        ZERO_TOTAL_AMOUNT
    }

    // 1. CSV解析工具
    private static List<String> parseCsv(String line) {

        List<String> result = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes
                        && i + 1 < line.length()
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

    // 根据字段名查找CSV列下标
    private static int findColumn(
            List<String> header, String name)
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

    // 判断是否为缺失值
    private static boolean isMissing(String value) {

        String s = value.trim();

        return s.isEmpty()
                || s.equalsIgnoreCase("null")
                || s.equalsIgnoreCase("nan")
                || s.equalsIgnoreCase("na")
                || s.equals("\\N");
    }

    // 解析资产金额，缺失值视为0
    private static BigDecimal parseAmount(String value) {

        String s = value.trim();

        if (isMissing(s)) {
            return BigDecimal.ZERO;
        }

        return new BigDecimal(s);
    }

    // 解析客户资产总额，与Task3保持一致
    private static double parseCustomerAsset(String value) {

        String s = value.trim();

        if (isMissing(s)) {
            return 0.0;
        }

        double result = Double.parseDouble(s);

        if (!Double.isFinite(result)) {
            throw new NumberFormatException("无效资产值");
        }

        return result;
    }

    // 2. 读取Task3计算的四分位数
    private static double[] readQuartiles() throws IOException {

        double[] quartiles = new double[3];
        boolean[] found = new boolean[3];

        // Task3结果已通过Distributed Cache本地化
        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(CACHE_NAME),
                StandardCharsets.UTF_8)) {

            String line;

            while ((line = reader.readLine()) != null) {

                if (line.trim().isEmpty()) {
                    continue;
                }

                // Task3输出格式:
                // Q25:数值 TAB Q50:数值 TAB Q75:数值
                String[] parts = line.trim().split("\\s+");

                for (String part : parts) {

                    try {
                        if (part.startsWith("Q25:")) {
                            quartiles[0] = Double.parseDouble(
                                    part.substring(4));
                            found[0] = true;

                        } else if (part.startsWith("Q50:")) {
                            quartiles[1] = Double.parseDouble(
                                    part.substring(4));
                            found[1] = true;

                        } else if (part.startsWith("Q75:")) {
                            quartiles[2] = Double.parseDouble(
                                    part.substring(4));
                            found[2] = true;
                        }
                    } catch (NumberFormatException e) {
                        throw new IOException(
                                "四分位数格式错误: " + part, e);
                    }
                }
            }
        }

        for (int i = 0; i < 3; i++) {
            if (!found[i] || !Double.isFinite(quartiles[i])) {
                throw new IOException(
                        "Task3四分位数缺失或无效");
            }
        }

        if (quartiles[0] > quartiles[1]
                || quartiles[1] > quartiles[2]) {
            throw new IOException(
                    "四分位数顺序错误: Q25 <= Q50 <= Q75");
        }

        return quartiles;
    }

    // 根据四分位数判断客户资产等级
    private static int getAssetLevel(
            double asset, double[] quartiles) {

        if (asset <= quartiles[0]) {
            return 0; // 低资产
        } else if (asset <= quartiles[1]) {
            return 1; // 中等资产
        } else if (asset <= quartiles[2]) {
            return 2; // 较高资产
        } else {
            return 3; // 高资产
        }
    }

    // 3. Mapper：客户分层并输出资产金额
    public static class Task4Mapper
            extends Mapper<LongWritable, Text,
                           IntWritable, Text> {

        private double[] quartiles;

        private int totalAssetIndex;
        private final int[] assetIndexes =
                new int[ASSET_COUNT];

        private int maxIndex;

        private final IntWritable outKey =
                new IntWritable();

        private final Text outValue = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            // 加载Task3结果中的Q25、Q50、Q75
            quartiles = readQuartiles();

            // 读取train.csv表头
            FileSplit split =
                    (FileSplit) context.getInputSplit();

            Path file = split.getPath();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            file.getFileSystem(
                                    context.getConfiguration())
                                    .open(file),
                            StandardCharsets.UTF_8))) {

                String firstLine = reader.readLine();

                if (firstLine == null) {
                    throw new IOException("train.csv为空");
                }

                List<String> header = parseCsv(firstLine);

                totalAssetIndex =
                        findColumn(header, "CUST_ASSET_AMT");

                maxIndex = totalAssetIndex;

                // 查找七类资产对应的列下标
                for (int i = 0; i < ASSET_COUNT; i++) {
                    assetIndexes[i] =
                            findColumn(header, ASSET_COLUMNS[i]);

                    maxIndex = Math.max(
                            maxIndex, assetIndexes[i]);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 跳过表头和空行
            if (key.get() == 0 || line.trim().isEmpty()) {
                return;
            }

            List<String> fields = parseCsv(line);

            if (fields.size() <= maxIndex) {
                context.getCounter(
                        DataCounter.INVALID_ROW).increment(1);
                return;
            }

            // 获取客户资产总额
            double totalAsset;

            try {
                totalAsset = parseCustomerAsset(
                        fields.get(totalAssetIndex));
            } catch (NumberFormatException e) {
                context.getCounter(
                        DataCounter.INVALID_ASSET)
                        .increment(1);
                return;
            }

            // 根据Task3的分位数确定资产等级
            int level = getAssetLevel(
                    totalAsset, quartiles);

            // 提取七类资产金额
            BigDecimal[] amounts =
                    new BigDecimal[ASSET_COUNT];

            try {
                for (int i = 0; i < ASSET_COUNT; i++) {
                    amounts[i] = parseAmount(
                            fields.get(assetIndexes[i]));
                }
            } catch (NumberFormatException e) {
                context.getCounter(
                        DataCounter.INVALID_CATEGORY_AMOUNT)
                        .increment(1);
                return;
            }

            // Mapper输出:
            // 等级编号 -> 七类资产金额
            //
            // 示例:
            // 0 -> 100,200,0,50,0,0,0
            outKey.set(level);
            outValue.set(encodeAmounts(amounts));

            context.write(outKey, outValue);
        }
    }

    // 4. 汇总工具
    // 创建7个初始金额为0的统计变量
    private static BigDecimal[] emptyAmounts() {

        BigDecimal[] result =
                new BigDecimal[ASSET_COUNT];

        Arrays.fill(result, BigDecimal.ZERO);

        return result;
    }

    // 将7个金额转换成逗号分隔的字符串
    private static String encodeAmounts(
            BigDecimal[] amounts) {

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < ASSET_COUNT; i++) {
            if (i > 0) {
                builder.append(",");
            }

            builder.append(amounts[i].toPlainString());
        }

        return builder.toString();
    }

    // 将Mapper或Combiner的结果进行累加
    private static BigDecimal[] sumAmounts(
            Iterable<Text> values) throws IOException {

        BigDecimal[] totals = emptyAmounts();

        for (Text value : values) {

            String[] parts =
                    value.toString().split(",", -1);

            if (parts.length != ASSET_COUNT) {
                throw new IOException(
                        "中间结果格式错误: " + value);
            }

            try {
                for (int i = 0; i < ASSET_COUNT; i++) {
                    totals[i] = totals[i].add(
                            new BigDecimal(parts[i]));
                }
            } catch (NumberFormatException e) {
                throw new IOException(
                        "中间结果金额解析失败: " + value, e);
            }
        }

        return totals;
    }

    // 5. Combiner：本地预聚合
    public static class Task4Combiner
            extends Reducer<IntWritable, Text,
                            IntWritable, Text> {

        @Override
        protected void reduce(
                IntWritable key,
                Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {

            // 汇总同一等级的七类资产
            BigDecimal[] totals = sumAmounts(values);

            context.write(
                    key,
                    new Text(encodeAmounts(totals))
            );
        }
    }

    // 6. Reducer：计算占比并排序
    public static class Task4Reducer
            extends Reducer<IntWritable, Text,
                            Text, Text> {

        @Override
        protected void reduce(
                IntWritable key,
                Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {

            // 当前资产等级
            int level = key.get();

            if (level < 0 || level >= LEVEL_NAMES.length) {
                throw new IOException("无效等级: " + level);
            }

            // 汇总该等级下七类资产的总额
            final BigDecimal[] totals =
                    sumAmounts(values);

            // 计算七类资产总额之和
            BigDecimal grandTotal = BigDecimal.ZERO;

            for (BigDecimal amount : totals) {
                grandTotal = grandTotal.add(amount);
            }

            final int denominatorSign = grandTotal.signum();

            if (denominatorSign == 0) {
                context.getCounter(
                        DataCounter.ZERO_TOTAL_AMOUNT)
                        .increment(1);
            }

            // 创建资产类型的排序下标
            Integer[] order = new Integer[ASSET_COUNT];

            for (int i = 0; i < ASSET_COUNT; i++) {
                order[i] = i;
            }

            // 按资产占比从大到小排序
            // 因为同一等级共用分母，
            // 分母为正时按金额降序即可。
            Arrays.sort(order, new Comparator<Integer>() {
                @Override
                public int compare(Integer a, Integer b) {

                    int result;

                    if (denominatorSign > 0) {
                        result = totals[b].compareTo(totals[a]);
                    } else if (denominatorSign < 0) {
                        result = totals[a].compareTo(totals[b]);
                    } else {
                        result = 0;
                    }

                    // 占比相同时按照原始类别顺序排列
                    return result != 0
                            ? result
                            : Integer.compare(a, b);
                }
            });

            String levelName = LEVEL_NAMES[level];

            // 按排序后的顺序输出
            for (int index : order) {

                BigDecimal ratio;

                if (denominatorSign == 0) {
                    // 分母为0时占比无法定义，
                    // 此处使用0并记录Counter
                    ratio = BigDecimal.ZERO.setScale(4);
                } else {
                    ratio = totals[index].divide(
                            grandTotal,
                            4,
                            RoundingMode.HALF_UP);
                }

                // 最终输出:
                // 等级 TAB 资产种类 TAB 占比
                context.write(
                        new Text(levelName),
                        new Text(
                                ASSET_NAMES[index]
                                + "\t"
                                + ratio.toPlainString())
                );
            }
        }
    }

    // 7. Driver：提交MapReduce任务
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "用法: Task4 <train.csv> "
                    + "<Task3结果文件> <Task4输出目录>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf, "Task4 - Asset Structure Analysis");

        job.setJarByClass(Task4.class);

        // Mapper
        job.setMapperClass(Task4Mapper.class);

        // Combiner：本地预聚合
        job.setCombinerClass(Task4Combiner.class);

        // Reducer：计算占比并排序
        job.setReducerClass(Task4Reducer.class);

        // Mapper输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 一个Reducer保证四个等级全局有序
        job.setNumReduceTasks(1);

        // 第一个参数：train.csv的HDFS路径
        FileInputFormat.addInputPath(
                job, new Path(args[0]));

        // 第二个参数：Task3结果文件的HDFS路径
        Path task3File = new Path(args[1]);
        FileSystem fs = task3File.getFileSystem(conf);

        if (!fs.isFile(task3File)) {
            throw new IOException(
                    "找不到Task3结果文件: " + args[1]);
        }

        // 将Task3结果加入分布式缓存
        Path qualifiedPath = fs.makeQualified(task3File);

        URI cacheUri = URI.create(
                qualifiedPath.toUri().toString()
                        + "#" + CACHE_NAME);

        job.addCacheFile(cacheUri);

        // 第三个参数：Task4输出目录
        FileOutputFormat.setOutputPath(
                job, new Path(args[2]));

        // 提交Job并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}