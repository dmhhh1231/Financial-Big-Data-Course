
import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


 //Task 1: 对每个 (指数编号, 股票代码) 判断其在 A、B 两个文件中的集合关系。
 //输出：指数编号<TAB>股票代码<TAB>BOTH/A_ONLY/B_ONLY
public class task1 {

    // 两个输入文件共用解析逻辑，由各自的 Mapper 标记数据来源。
    //Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    public static abstract class StockMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        protected abstract String sourceTag();

        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            // 1.行清洗与校验
            String record = line.toString().trim();
            if (record.isEmpty()) {
                return;
            }

            // 2.按空白符分割字段
            String[] fields = record.split("\\s+");
            if (fields.length != 2) {
                throw new IOException("Invalid stock record: " + record);
            }

            // 联合键保证同一指数内的股票按代码排序。
            String indexAndStock = fields[0] + "\t" + fields[1];
            context.write(new Text(indexAndStock), new Text(sourceTag()));
        }
    }

    public static class AFileMapper extends StockMapper {
        @Override
        protected String sourceTag() {
            return "A";
        }
    }

    public static class BFileMapper extends StockMapper {
        @Override
        protected String sourceTag() {
            return "B";
        }
    }

    // Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    public static class RelationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text indexAndStock, Iterable<Text> sources, Context context)
                throws IOException, InterruptedException {
            boolean inA = false;
            boolean inB = false;

            // 遍历所有来源标记，只判断「有没有」，不统计「有多少」
            for (Text source : sources) {
                if ("A".equals(source.toString())) {
                    inA = true;
                } else if ("B".equals(source.toString())) {
                    inB = true;
                } else {
                    throw new IOException("Unknown source: " + source);
                }
            }

            // 判定集合关系
            String relation = inA && inB ? "BOTH" : inA ? "A_ONLY" : "B_ONLY";
            context.write(indexAndStock, new Text(relation));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: task1 <A.txt> <B.txt> <output_task1>");
            System.exit(1);
        }

        Job job = Job.getInstance(new Configuration(), "homework5-task1");
        job.setJarByClass(task1.class);

        // 显式指定来源，避免仅凭文件名推断 A/B。
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, AFileMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, BFileMapper.class);

        job.setReducerClass(RelationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 单个 Reducer 生成按 (指数编号, 股票代码) 排序的 part-r-00000。
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}