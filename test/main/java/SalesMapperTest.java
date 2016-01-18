import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

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
        mapDriver.withInput(new LongWritable(1),new Text("Jan-17\tJanice\tJeans\tBangalore\t1500"));
        mapDriver.withOutput(new Text("Bangalore"),new IntWritable(1500));
        mapDriver.runTest();
    }
}