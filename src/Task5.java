
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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

public class Task5 {

    // KNN默认参数
    private static final int DEFAULT_K = 5;
    private static final double EPSILON = 1e-6;

    // 分布式缓存中的训练文件名称
    private static final String CACHE_FILE = "task5_train.csv";

    // 10个数值特征 + 2个标志特征
    private static final int NUMERIC_COUNT = 10;
    private static final int FEATURE_COUNT = 12;

    // 数值特征名称
    private static final String[] NUMERIC_COLUMNS = {
        "CUST_SAVING_AMT",
        "CUST_FINA_AMT",
        "CUST_PRIFINA_AMT",
        "CUST_FOND_AMT",
        "CUST_NADEBT_AMT",
        "CUST_YBT_AMT",
        "CUST_METAL_AMT",
        "CUST_ASSET_AMT",
        "CUST_DEBT_AMT",
        "G_OS_PRCP_SUM"
    };

    public enum DataCounter {
        INVALID_TEST_ROW,
        MISSING_CUSTOMER_ID,
        PREDICTED_COUNT
    }

    // 1. CSV解析工具

    // 支持普通CSV中的引号和空字段
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

    // 根据字段名称查找列下标
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

        throw new IOException("找不到CSV字段: " + name);
    }

    // 安全获取字段
    private static String getField(
            List<String> fields, int index) {

        if (index < 0 || index >= fields.size()) {
            return "";
        }

        return fields.get(index).trim();
    }

    private static boolean isMissing(String value) {

        return value.isEmpty()
                || value.equalsIgnoreCase("null")
                || value.equalsIgnoreCase("nan")
                || value.equalsIgnoreCase("na")
                || value.equals("\\N");
    }

    // 数值特征：缺失值按0处理，并做log1p变换
    private static double parseNumeric(String value) {

        String s = value.trim();

        if (isMissing(s)) {
            return 0.0;
        }

        try {
            double number = Double.parseDouble(s);

            if (!Double.isFinite(number)) {
                return 0.0;
            }

            return Math.copySign(
                    Math.log1p(Math.abs(number)),
                    number
            );

        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    // 标志特征：Y或1表示1，其他表示0
    private static double parseFlag(String value) {

        String s = value.trim();

        return (s.equalsIgnoreCase("Y")
                || s.equals("1")
                || s.equalsIgnoreCase("true"))
                ? 1.0 : 0.0;
    }

    // 2. 字段索引管理
    private static class Schema {

        int idIndex;
        int labelIndex;

        int[] numericIndexes =
                new int[NUMERIC_COUNT];

        int loanFlagIndex;
        int credFlagIndex;

        Schema(String headerLine, boolean training)
                throws IOException {

            List<String> header = parseCsv(headerLine);

            idIndex = findColumn(header, "CUST_ID");

            // 测试集不需要bad_good标签
            labelIndex = training
                    ? findColumn(header, "bad_good")
                    : -1;

            for (int i = 0; i < NUMERIC_COUNT; i++) {
                numericIndexes[i] =
                        findColumn(header, NUMERIC_COLUMNS[i]);
            }

            loanFlagIndex =
                    findColumn(header, "LOAN_FLAG");

            credFlagIndex =
                    findColumn(header, "CRED_FLAG");
        }
    }

    // 从CSV记录中提取12维特征
    private static double[] extractFeatures(
            List<String> fields, Schema schema) {

        double[] features =
                new double[FEATURE_COUNT];

        for (int i = 0; i < NUMERIC_COUNT; i++) {
            features[i] = parseNumeric(
                    getField(fields, schema.numericIndexes[i]));
        }

        features[10] = parseFlag(
                getField(fields, schema.loanFlagIndex));

        features[11] = parseFlag(
                getField(fields, schema.credFlagIndex));

        return features;
    }

    // 读取HDFS输入文件的CSV表头
    private static String readHeader(
            Path file, Configuration conf)
            throws IOException {

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        file.getFileSystem(conf).open(file),
                        StandardCharsets.UTF_8))) {

            String header = reader.readLine();

            if (header == null) {
                throw new IOException("CSV文件为空: " + file);
            }

            return header;
        }
    }

    // 3. KNN训练样本与邻居结构
    private static class TrainSample {

        double[] features;
        int label;

        TrainSample(double[] features, int label) {
            this.features = features;
            this.label = label;
        }
    }

    private static class Neighbor {

        double distanceSquared;
        int label;

        Neighbor(double distanceSquared, int label) {
            this.distanceSquared = distanceSquared;
            this.label = label;
        }
    }

    // 4. Mapper：完整KNN预测过程
    public static class KNNMapper
            extends Mapper<LongWritable, Text,
                           Text, IntWritable> {

        // 每个Mapper加载完整训练集
        private final List<TrainSample> trainingData =
                new ArrayList<>();

        private final double[] means =
                new double[NUMERIC_COUNT];

        private final double[] stddev =
                new double[NUMERIC_COUNT];

        private Schema testSchema;
        private int k;

        private final Text outKey = new Text();
        private final IntWritable outValue =
                new IntWritable();

        // ---------------------------------
        // setup：从DistributedCache加载训练集
        // ---------------------------------

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            k = conf.getInt("task5.knn.k", DEFAULT_K);

            // Hadoop已将训练CSV缓存到Mapper工作目录
            try (BufferedReader reader =
                    Files.newBufferedReader(
                            Paths.get(CACHE_FILE),
                            StandardCharsets.UTF_8)) {

                String headerLine = reader.readLine();

                if (headerLine == null) {
                    throw new IOException("训练集为空");
                }

                Schema trainSchema =
                        new Schema(headerLine, true);

                String line;

                // 读取完整训练集
                while ((line = reader.readLine()) != null) {

                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    List<String> fields = parseCsv(line);

                    String labelText =
                            getField(fields,
                                    trainSchema.labelIndex);

                    // 只保留标签为0或1的样本
                    if (!labelText.equals("0")
                            && !labelText.equals("1")) {
                        continue;
                    }

                    int label =
                            Integer.parseInt(labelText);

                    double[] features =
                            extractFeatures(fields, trainSchema);

                    trainingData.add(
                            new TrainSample(features, label));
                }
            }

            if (trainingData.size() < k) {
                throw new IOException(
                        "有效训练样本数量小于K，无法预测");
            }

            // 计算训练集的均值和标准差
            computeNormalization();

            // 将训练集特征标准化
            for (TrainSample sample : trainingData) {
                normalize(sample.features);
            }

            // 读取当前测试文件表头
            FileSplit split =
                    (FileSplit) context.getInputSplit();

            Path testFile = split.getPath();

            testSchema = new Schema(
                    readHeader(testFile, conf),
                    false);
        }

        // 根据完整训练集计算标准化参数
        private void computeNormalization() {

            int n = trainingData.size();

            // 均值
            for (TrainSample sample : trainingData) {
                for (int i = 0; i < NUMERIC_COUNT; i++) {
                    means[i] += sample.features[i];
                }
            }

            for (int i = 0; i < NUMERIC_COUNT; i++) {
                means[i] /= n;
            }

            // 方差
            for (TrainSample sample : trainingData) {
                for (int i = 0; i < NUMERIC_COUNT; i++) {

                    double diff =
                            sample.features[i] - means[i];

                    stddev[i] += diff * diff;
                }
            }

            // 标准差
            for (int i = 0; i < NUMERIC_COUNT; i++) {

                stddev[i] = Math.sqrt(stddev[i] / n);

                if (stddev[i] < 1e-9) {
                    stddev[i] = 1.0;
                }
            }
        }

        // 训练集和测试集使用相同的标准化参数
        private void normalize(double[] features) {

            for (int i = 0; i < NUMERIC_COUNT; i++) {
                features[i] =
                        (features[i] - means[i]) / stddev[i];
            }

            // 后两个二值特征保持0/1
        }

        // 计算欧氏距离平方
        private double distanceSquared(
                double[] a,
                double[] b,
                double cutoff) {

            double sum = 0.0;

            for (int i = 0; i < FEATURE_COUNT; i++) {

                double diff = a[i] - b[i];

                sum += diff * diff;

                // 已比第K近邻更远时提前终止计算
                if (sum > cutoff) {
                    break;
                }
            }

            return sum;
        }

        // 对一个测试样本执行加权KNN
        private int predict(double[] testFeatures) {

            // 大顶堆：堆顶为当前最远的邻居
            PriorityQueue<Neighbor> topK =
                    new PriorityQueue<>(
                            k,
                            new Comparator<Neighbor>() {
                                @Override
                                public int compare(
                                        Neighbor a,
                                        Neighbor b) {

                                    return Double.compare(
                                            b.distanceSquared,
                                            a.distanceSquared);
                                }
                            });

            // 遍历完整训练集
            for (TrainSample sample : trainingData) {

                double cutoff =
                        topK.size() < k
                                ? Double.POSITIVE_INFINITY
                                : topK.peek().distanceSquared;

                double d2 = distanceSquared(
                        testFeatures,
                        sample.features,
                        cutoff);

                // 维护距离最小的K个样本
                // 相当于保留相似度最大的K个样本
                if (topK.size() < k) {

                    topK.offer(new Neighbor(
                            d2, sample.label));

                } else if (d2
                        < topK.peek().distanceSquared) {

                    topK.poll();

                    topK.offer(new Neighbor(
                            d2, sample.label));
                }
            }

            // ---------------------------------
            // 相似度加权投票
            // S = 1 / (distance + epsilon)
            // ---------------------------------

            double weight0 = 0.0;
            double weight1 = 0.0;

            double closestDistance =
                    Double.POSITIVE_INFINITY;

            int closestLabel = 0;

            for (Neighbor neighbor : topK) {

                double distance =
                        Math.sqrt(neighbor.distanceSquared);

                double similarity =
                        1.0 / (distance + EPSILON);

                if (neighbor.label == 0) {
                    weight0 += similarity;
                } else {
                    weight1 += similarity;
                }

                // 记录最近邻，用于极少数平票情况
                if (neighbor.distanceSquared
                        < closestDistance) {

                    closestDistance =
                            neighbor.distanceSquared;

                    closestLabel = neighbor.label;
                }
            }

            if (Math.abs(weight1 - weight0) < 1e-12) {
                return closestLabel;
            }

            return weight1 > weight0 ? 1 : 0;
        }

        // map：预测单条测试记录
        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 跳过表头与空行
            if (key.get() == 0 || line.trim().isEmpty()) {
                return;
            }

            List<String> fields = parseCsv(line);

            // 获取测试客户ID
            String customerId =
                    getField(fields, testSchema.idIndex);

            if (customerId.isEmpty()) {

                context.getCounter(
                        DataCounter.MISSING_CUSTOMER_ID)
                        .increment(1);

                return;
            }

            // 提取测试样本特征
            double[] testFeatures =
                    extractFeatures(fields, testSchema);

            // 使用训练集的均值和标准差
            normalize(testFeatures);

            // 在Mapper中完成KNN预测
            int predictedLabel = predict(testFeatures);

            // 输出：(客户ID, 预测标签)
            outKey.set(customerId);
            outValue.set(predictedLabel);

            context.write(outKey, outValue);

            context.getCounter(
                    DataCounter.PREDICTED_COUNT)
                    .increment(1);
        }
    }

    // 5. Reducer：直接输出，不聚合
    public static class KNNReducer
            extends Reducer<Text, IntWritable,
                            Text, IntWritable> {

        @Override
        protected void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            // Mapper已经完成预测
            // Reducer直接写出，不做二次投票或求和
            for (IntWritable value : values) {
                context.write(key, value);
            }
        }
    }

    // 6. Driver：配置并提交MapReduce任务
    public static void main(String[] args) throws Exception {

        if (args.length != 3 && args.length != 4) {

            System.err.println(
                    "用法: Task5 "
                    + "<train.csv> <test.csv> "
                    + "<output_dir> [K]");

            System.exit(1);
        }

        // 默认K=5，也允许通过第四个参数修改
        int k = DEFAULT_K;

        if (args.length == 4) {
            k = Integer.parseInt(args[3]);
        }

        if (k < 1) {
            throw new IllegalArgumentException(
                    "K必须是正整数");
        }

        Configuration conf = new Configuration();
        conf.setInt("task5.knn.k", k);

        Job job = Job.getInstance(
                conf,
                "Task5 - Distributed KNN Prediction");

        job.setJarByClass(Task5.class);

        // Mapper：进行完整KNN计算
        job.setMapperClass(KNNMapper.class);

        // Reducer：直接输出预测结果
        job.setReducerClass(KNNReducer.class);

        // Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 只需要一个Reducer，便于生成单个结果文件
        job.setNumReduceTasks(1);

        // 测试数据作为MapReduce输入
        Path testPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, testPath);

        // 设置输入分块上限8MB
        // 使较大的测试文件产生多个Mapper任务
        FileInputFormat.setMaxInputSplitSize(
                job,
                8L * 1024 * 1024);

        // 将完整训练集加入DistributedCache
        Path trainPath = new Path(args[0]);

        FileSystem fs =
                trainPath.getFileSystem(conf);

        if (!fs.isFile(trainPath)) {
            throw new IOException(
                    "找不到训练数据: " + trainPath);
        }

        Path qualifiedPath =
                fs.makeQualified(trainPath);

        URI cacheUri = URI.create(
                qualifiedPath.toUri().toString()
                + "#" + CACHE_FILE);

        // Hadoop自动将训练文件本地化到执行节点
        job.addCacheFile(cacheUri);

        // 最终输出目录
        FileOutputFormat.setOutputPath(
                job, new Path(args[2]));

        System.out.println("KNN K = " + k);

        // 提交MapReduce任务
        System.exit(
                job.waitForCompletion(true) ? 0 : 1);
    }
}