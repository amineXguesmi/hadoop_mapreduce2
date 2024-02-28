package hadoop.mapreduce.tp1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SalesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text store = new Text();
    private final static IntWritable saleAmount = new IntWritable(0);

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\t");

        for (int i = 0; i < 2; i++) {
            if (itr.hasMoreTokens()) {
                itr.nextToken();
            } else {
                return;
            }
        }

        if (itr.hasMoreTokens()) {
            store.set(itr.nextToken());
        } else {
            return;
        }

        for (int i = 0; i < 1; i++) {
            if (itr.hasMoreTokens()) {
                itr.nextToken();
            } else {
                return;
            }
        }

        if (itr.hasMoreTokens()) {
            double amount = Double.parseDouble(itr.nextToken());
            saleAmount.set((int)amount);
            context.write(store, saleAmount);
        }
    }
}
