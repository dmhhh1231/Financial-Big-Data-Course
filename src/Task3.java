
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Task3 {

    // 用于在Hadoop日志中记录数据处理情况
    public enum AssetCounter {
        VALID_ASSET,
        INVALID_ASSET,
        INVALID_ROW
    }

    private static final String COUNT_KEY = "task3.asset.count";

    // 1. CSV解析工具
    // 支持双引号和空字段，不支持跨行CSV字段
    private static List<String> parseCsv(String line) {

        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        fields.add(current.toString().trim());
        return fields;
    }

    // 根据CSV表头寻找资产总额所在列
    private static int findAssetColumn(String headerLine)
            throws IOException {

        List<String> headers = parseCsv(headerLine);

        for (int i = 0; i < headers.size(); i++) {
            String column = headers.get(i)
                    .replace("\uFEFF", "").trim();

            if (column.equalsIgnoreCase("CUST_ASSET_AMT")) {
                return i;
            }
        }

        throw new IOException(
                "CSV中未找到CUST_ASSET_AMT字段");
    }

    // 判断是否属于缺失资产
    private static boolean isMissing(String value) {

        return value.isEmpty()
                || value.equalsIgnoreCase("null")
                || value.equalsIgnoreCase("nan")
                || value.equalsIgnoreCase("na")
                || value.equals("\\N");
    }

    // 2. 公共Mapper：读取资产数据
    // 两个Job共用同一套CSV解析逻辑，
    // 保证统计数量和排序使用相同的数据。
    public static abstract class AssetMapper<K, V>
            extends Mapper<LongWritable, Text, K, V> {

        private int assetIndex;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            FileSplit split =
                    (FileSplit) context.getInputSplit();

            Path file = split.getPath();

            // 每个Mapper读取CSV第一行，确定字段位置
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    file.getFileSystem(
                                            context.getConfiguration())
                                            .open(file),
                                    StandardCharsets.UTF_8))) {

                String header = reader.readLine();

                if (header == null) {
                    throw new IOException("CSV文件为空");
                }

                assetIndex = findAssetColumn(header);
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

            if (fields.size() <= assetIndex) {
                context.getCounter(
                        AssetCounter.INVALID_ROW).increment(1);
                return;
            }

            String assetText =
                    fields.get(assetIndex).trim();

            double asset;

            // 缺失资产按照0处理
            if (isMissing(assetText)) {
                asset = 0.0;
            } else {
                try {
                    asset = Double.parseDouble(assetText);

                    // 排除无穷大等非法数值
                    if (!Double.isFinite(asset)) {
                        throw new NumberFormatException(
                                "非有限数值");
                    }

                } catch (NumberFormatException e) {
                    context.getCounter(
                            AssetCounter.INVALID_ASSET)
                            .increment(1);
                    return;
                }
            }

            processAsset(asset, context);
        }

        // 由具体Mapper实现
        protected abstract void processAsset(
                double asset, Context context)
                throws IOException, InterruptedException;
    }

    // 3. Job1：统计总样本数量
    public static class CountMapper
            extends AssetMapper<NullWritable, NullWritable> {

        @Override
        protected void processAsset(
                double asset, Context context) {

            // 每个有效资产记录计数一次
            context.getCounter(
                    AssetCounter.VALID_ASSET).increment(1);
        }
    }

    // 4. Job2 Mapper：输出资产作为Key
    public static class SortMapper
            extends AssetMapper<DoubleWritable, NullWritable> {

        private final DoubleWritable outKey =
                new DoubleWritable();

        @Override
        protected void processAsset(
                double asset, Context context)
                throws IOException, InterruptedException {

            // 资产总额作为Key
            // Hadoop Shuffle会自动按数值升序排序
            outKey.set(asset);

            context.write(
                    outKey,
                    NullWritable.get()
            );
        }
    }

    // 5. Job2 Reducer：计算四分位数
    public static class QuartileReducer
            extends Reducer<DoubleWritable, NullWritable,
                            NullWritable, Text> {

        private long totalCount;
        private long processed = 0;

        // 三个分位数的上下位置，均从0开始
        private final long[] lowerIndex = new long[3];
        private final long[] upperIndex = new long[3];

        // 插值时使用的余数，分母固定为4
        private final int[] remainder = new int[3];

        private final double[] lowerValue = new double[3];
        private final double[] upperValue = new double[3];

        private final boolean[] lowerFound = new boolean[3];
        private final boolean[] upperFound = new boolean[3];

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            totalCount = context.getConfiguration()
                    .getLong(COUNT_KEY, -1);

            if (totalCount <= 0) {
                throw new IOException("资产样本数量无效");
            }

            // 线性插值分位数位置：
            // Q25 = (N-1) * 1/4
            // Q50 = (N-1) * 2/4
            // Q75 = (N-1) * 3/4
            for (int i = 0; i < 3; i++) {

                long numerator =
                        (totalCount - 1) * (i + 1);

                lowerIndex[i] = numerator / 4;

                remainder[i] = (int) (numerator % 4);

                upperIndex[i] = lowerIndex[i];

                if (remainder[i] != 0) {
                    upperIndex[i]++;
                }
            }
        }

        @Override
        protected void reduce(
                DoubleWritable key,
                Iterable<NullWritable> values,
                Context context)
                throws IOException, InterruptedException {

            // Hadoop已经将不同资产Key按升序排列。
            // 相同资产数值的记录会被分到同一个组。
            long frequency = 0;

            for (NullWritable ignored : values) {
                frequency++;
            }

            // 当前资产值在全局排序序列中的位置范围
            long start = processed;
            long end = processed + frequency;

            double asset = key.get();

            for (int i = 0; i < 3; i++) {

                // 捕获分位数下界位置的资产值
                if (lowerIndex[i] >= start
                        && lowerIndex[i] < end) {

                    lowerValue[i] = asset;
                    lowerFound[i] = true;
                }

                // 捕获分位数上界位置的资产值
                if (upperIndex[i] >= start
                        && upperIndex[i] < end) {

                    upperValue[i] = asset;
                    upperFound[i] = true;
                }
            }

            // 累计已处理的资产记录数量
            processed = end;
        }

        // 对上下两个位置进行线性插值
        private String getQuantile(int index) {

            BigDecimal lower =
                    BigDecimal.valueOf(lowerValue[index]);

            BigDecimal upper =
                    BigDecimal.valueOf(upperValue[index]);

            BigDecimal difference =
                    upper.subtract(lower);

            BigDecimal fraction =
                    BigDecimal.valueOf(remainder[index])
                            .divide(BigDecimal.valueOf(4));

            BigDecimal result =
                    lower.add(difference.multiply(fraction));

            return result.stripTrailingZeros().toPlainString();
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            // 检查排序统计的样本数量是否一致
            if (processed != totalCount) {
                throw new IOException(
                        "样本数量不一致: 预期="
                        + totalCount + ", 实际=" + processed);
            }

            for (int i = 0; i < 3; i++) {
                if (!lowerFound[i] || !upperFound[i]) {
                    throw new IOException(
                            "未能获取全部四分位数位置");
                }
            }

            String result =
                    "Q25:" + getQuantile(0)
                    + "\tQ50:" + getQuantile(1)
                    + "\tQ75:" + getQuantile(2);

            // NullWritable不会额外输出Key
            // 最终只输出一行
            context.write(
                    NullWritable.get(),
                    new Text(result)
            );
        }
    }

    // 6. Driver：依次执行两个MapReduce Job
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "用法: Task3 <train.csv> <output_path>");
            System.exit(1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // ---------- Job1：统计样本数量 ----------

        Configuration countConf = new Configuration();

        Job countJob = Job.getInstance(
                countConf, "Task3 - Count Asset Records");

        countJob.setJarByClass(Task3.class);
        countJob.setMapperClass(CountMapper.class);

        // 只需要Mapper和Counter
        countJob.setNumReduceTasks(0);

        // 不产生HDFS输出文件
        countJob.setOutputFormatClass(
                NullOutputFormat.class);

        countJob.setMapOutputKeyClass(
                NullWritable.class);

        countJob.setMapOutputValueClass(
                NullWritable.class);

        countJob.setOutputKeyClass(
                NullWritable.class);

        countJob.setOutputValueClass(
                NullWritable.class);

        FileInputFormat.addInputPath(
                countJob, inputPath);

        if (!countJob.waitForCompletion(true)) {
            System.err.println("Task3计数Job执行失败");
            System.exit(1);
        }

        // 从Hadoop Counter获得有效样本总数
        long totalCount = countJob.getCounters()
                .findCounter(AssetCounter.VALID_ASSET)
                .getValue();

        if (totalCount <= 0) {
            throw new IOException("没有可用于计算的资产数据");
        }

        System.out.println(
                "有效资产样本数量: " + totalCount);

        // ---------- Job2：全局排序并计算分位数 ----------

        Configuration sortConf = new Configuration();

        // 将样本数量传递给Reducer
        sortConf.setLong(COUNT_KEY, totalCount);

        Job sortJob = Job.getInstance(
                sortConf, "Task3 - Asset Quartiles");

        sortJob.setJarByClass(Task3.class);

        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(QuartileReducer.class);

        // 资产数值作为Key，自动进行数值排序
        sortJob.setMapOutputKeyClass(
                DoubleWritable.class);

        sortJob.setMapOutputValueClass(
                NullWritable.class);

        sortJob.setOutputKeyClass(
                NullWritable.class);

        sortJob.setOutputValueClass(Text.class);

        // 只使用一个Reducer，确保全局有序
        sortJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(
                sortJob, inputPath);

        FileOutputFormat.setOutputPath(
                sortJob, outputPath);

        // 等待排序和四分位数计算完成
        System.exit(
                sortJob.waitForCompletion(true) ? 0 : 1);
    }
}