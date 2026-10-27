
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1 {

    // 记录异常数据，便于在运行日志中查看
    public enum DataCounter {
        MISSING_ORG,
        INVALID_AMOUNT,
        UNKNOWN_LABEL,
        INVALID_ROW
    }

    // 解析CSV，保留空字段并支持双引号
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

    // 根据字段名称查找列下标
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

    // 金额缺失时按照0处理
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
    public static class Task1Mapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private int orgIndex;
        private int assetIndex;
        private int debtIndex;
        private int labelIndex;
        private int maxIndex;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            // 读取当前CSV文件的表头
            // 每个Mapper都可以获得正确的字段位置
            FileSplit split = (FileSplit) context.getInputSplit();
            Path file = split.getPath();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            file.getFileSystem(context.getConfiguration())
                                    .open(file),
                            StandardCharsets.UTF_8))) {

                String firstLine = reader.readLine();

                if (firstLine == null) {
                    throw new IOException("输入CSV文件为空");
                }

                List<String> header = parseCsv(firstLine);

                orgIndex = findColumn(header, "OPEN_ORG_NUM");
                assetIndex = findColumn(header, "CUST_ASSET_AMT");
                debtIndex = findColumn(header, "CUST_DEBT_AMT");
                labelIndex = findColumn(header, "bad_good");

                maxIndex = Math.max(
                        Math.max(orgIndex, assetIndex),
                        Math.max(debtIndex, labelIndex)
                );
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 跳过文件开头的表头和空行
            if (key.get() == 0 || line.trim().isEmpty()) {
                return;
            }

            List<String> fields = parseCsv(line);

            if (fields.size() <= maxIndex) {
                context.getCounter(DataCounter.INVALID_ROW)
                        .increment(1);
                return;
            }

            // 获取开户机构编号
            String org = fields.get(orgIndex).trim();

            if (org.isEmpty()) {
                context.getCounter(DataCounter.MISSING_ORG)
                        .increment(1);
                return;
            }

            BigDecimal asset;
            BigDecimal debt;

            try {
                asset = parseAmount(fields.get(assetIndex));
                debt = parseAmount(fields.get(debtIndex));
            } catch (NumberFormatException e) {
                context.getCounter(DataCounter.INVALID_AMOUNT)
                        .increment(1);
                return;
            }

            // 暂按 bad_good = 1 表示逾期
            String label = fields.get(labelIndex).trim();

            if (!label.equals("0") && !label.equals("1")) {
                context.getCounter(DataCounter.UNKNOWN_LABEL)
                        .increment(1);
            }

            long overdue = label.equals("1") ? 1L : 0L;

            // 每条有效记录代表一个客户
            // 输出：
            // 机构编号 -> 1,资产,负债,逾期标记
            String result = "1,"
                    + asset.toPlainString() + ","
                    + debt.toPlainString() + ","
                    + overdue;

            context.write(new Text(org), new Text(result));
        }
    }

    // 2. 统计结果辅助类
    private static class Stats {

        long count = 0;
        BigDecimal totalAsset = BigDecimal.ZERO;
        BigDecimal totalDebt = BigDecimal.ZERO;
        long overdueCount = 0;

        void add(String value) throws IOException {
            String[] parts = value.split(",", -1);

            if (parts.length != 4) {
                throw new IOException("无效的中间结果: " + value);
            }

            try {
                count += Long.parseLong(parts[0]);
                totalAsset = totalAsset.add(
                        new BigDecimal(parts[1]));
                totalDebt = totalDebt.add(
                        new BigDecimal(parts[2]));
                overdueCount += Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                throw new IOException("中间结果解析失败: " + value, e);
            }
        }

        // Combiner使用：保留金额原始精度
        String intermediateValue() {
            return count + ","
                    + totalAsset.toPlainString() + ","
                    + totalDebt.toPlainString() + ","
                    + overdueCount;
        }

        // 最终输出：金额保留两位小数
        String outputValue() {
            return count + ","
                    + totalAsset.setScale(2, RoundingMode.HALF_UP)
                            .toPlainString() + ","
                    + totalDebt.setScale(2, RoundingMode.HALF_UP)
                            .toPlainString() + ","
                    + overdueCount;
        }
    }

    private static Stats aggregate(Iterable<Text> values)
            throws IOException {

        Stats stats = new Stats();

        for (Text value : values) {
            stats.add(value.toString());
        }

        return stats;
    }

    // 3. Combiner：本地预聚合
    public static class Task1Combiner
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            Stats stats = aggregate(values);

            context.write(
                    key,
                    new Text(stats.intermediateValue())
            );
        }
    }

    // 4. Reducer：汇总机构数据
    public static class Task1Reducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            Stats stats = aggregate(values);

            context.write(
                    key,
                    new Text(stats.outputValue())
            );
        }
    }

    // 5. Driver：提交MapReduce任务
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "用法: Task1 <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf, "Task1 - Customer Statistics by Organization");

        job.setJarByClass(Task1.class);

        // 设置Mapper、Combiner和Reducer
        job.setMapperClass(Task1Mapper.class);
        job.setCombinerClass(Task1Combiner.class);
        job.setReducerClass(Task1Reducer.class);

        // Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 使用一个Reducer，方便得到单个结果文件
        job.setNumReduceTasks(1);

        // HDFS输入与输出路径
        FileInputFormat.addInputPath(
                job, new Path(args[0]));

        FileOutputFormat.setOutputPath(
                job, new Path(args[1]));

        // 等待任务执行完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}