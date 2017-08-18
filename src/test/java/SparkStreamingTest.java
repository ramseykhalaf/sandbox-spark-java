import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SparkStreamingTest {

    public void testDStream() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("test");
        SparkContext sparkContext = new SparkContext(sparkConf);

    }
}
