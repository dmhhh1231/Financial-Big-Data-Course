
import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


 //Task 2: 读取任务 1 的输出，按指数统计交集、A 独有、B 独有、并集数量。
 //输出：指数编号<TAB>交集数<TAB>A独有数<TAB>B独有数<TAB>并集数
public class task2 {

    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            String record = line.toString().trim();
            if (record.isEmpty()) {
                return;
            }

            // 任务 1 的输出为：指数编号 股票代码 关系标记。
            String[] fields = record.split("\\s+");
            if (fields.length != 3) {
                throw new IOException("Invalid task1 output: " + record);
            }

            String relation = fields[2];
            if (!"BOTH".equals(relation)
                    && !"A_ONLY".equals(relation)
                    && !"B_ONLY".equals(relation)) {
                throw new IOException("Unknown relation: " + relation);
            }

            // 输出键：指数编号；输出值：关系标记
            context.write(new Text(fields[0]), new Text(relation));
        }
    }

    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text index, Iterable<Text> relations, Context context)
                throws IOException, InterruptedException {
            long both = 0;
            long aOnly = 0;
            long bOnly = 0;

            // 遍历该指数下所有股票的关系标记，分类累加
            for (Text value : relations) {
                switch (value.toString()) {
                    case "BOTH":
                        both++;
                        break;
                    case "A_ONLY":
                        aOnly++;
                        break;
                    case "B_ONLY":
                        bOnly++;
                        break;
                    default:
                        throw new IOException("Unknown relation: " + value);
                }
            }

            long union = both + aOnly + bOnly;
            context.write(index,
                    new Text(both + "\t" + aOnly + "\t" + bOnly + "\t" + union));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: task2 <output_task1> <output_task2>");
            System.exit(1);
        }

        Job job = Job.getInstance(new Configuration(), "homework5-task2");
        job.setJarByClass(task2.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 单个 Reducer 保证所有指数记录按编号排序。
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}