import com.sun.xml.bind.annotation.XmlLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


//计算sub PageRank
public class UnitMultiplication {

    //Transition Matrix Mapper
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability

            //输入
            //input: fromId \t to1, to2, to3...

            //输出
            //outputKey: fromId
            //outputValue: toId = probability
            String[] fromTos = value.toString().trim().split("\t");

            //when encounter dead end and spider trap, return
            if(fromTos.length == 1 || fromTos[1].trim().equals("")) {
                return;
            }

            String[] tos = fromTos[1].split(",");
            String from = fromTos[0];
            for(String to : tos){
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
            }
        }
    }


    //PageRank Matrix Mapper
    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to

            //outputKey : id
            //outputValue: pr(n)

            String[] idPr = value.toString().trim().split("\t");
            context.write(new Text(idPr[0]), new Text(idPr[1]));
        }
    }


    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        float beta;

        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }


        /**
        input:
        key = fromPage
        value=<toPage=probability..., pageRank>

        output:
        key =  toId
        value = subPR  -- probability * pr(n)

        target: get the unit multiplication
         **/
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //List表示transition matrix 的cell
            List<String> transitionUnit = new ArrayList<String>();
            //prUnit表示PR Cell
            double prUnit = 0;

            for (Text value: values) {
                //利用=区别数据额是来自transition matrix cell还是PageRank cell
                if(value.toString().contains("=")) {
                    transitionUnit.add(value.toString());
                }
                else {
                    prUnit = Double.parseDouble(value.toString());
                }
            }
            for (String unit: transitionUnit) {
                String outputKey = unit.split("=")[0];
                double relation = Double.parseDouble(unit.split("=")[1]);

                //transition matrix * pageRank matrix * (1-beta)
                //即subPR
                String outputValue = String.valueOf(relation * prUnit * (1-beta));
                //直接写出
                context.write(new Text(outputKey), new Text(outputValue));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //通知MapReduce我有多个Mapper，并且不同的Mapper需要从哪里读取input
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
