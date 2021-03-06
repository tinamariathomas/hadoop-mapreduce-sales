import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SalesMapperTest {
    MapDriver mapDriver;
    SalesMapper mapper;
    @Before
    public void setUp() {
        mapper = new SalesMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

    }
    @Test
    public void testMap() throws IOException {
        mapDriver.withInput(new LongWritable(1),new Text("Jan-17,Janice,Jeans,Bangalore,1500"));
        mapDriver.withOutput(new Text("Bangalore"),new IntWritable(1500));
        mapDriver.runTest();
    }
}