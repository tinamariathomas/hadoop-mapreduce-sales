import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SalesReducerTest {
    ReduceDriver reduceDriver;
    SalesReducer reducer;

    @Before
    public void setUp() {
        reducer = new SalesReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1500));
        values.add(new IntWritable(2000));
        FloatWritable expectedOutput = new FloatWritable(3500/2);
        reduceDriver.withInput(new Text("Bangalore"), values);
        reduceDriver.withOutput(new Text("Bangalore"), expectedOutput);
        reduceDriver.runTest();

    }
}