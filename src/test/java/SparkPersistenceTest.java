import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class SparkPersistenceTest {

    private SparkContext sc;
    private JavaSparkContext jsc;
    private HiveContext hiveContext;
    private SparkPersistence spark;
    private JavaStreamingContext streamingContext;

    @Before
    public void initSpark() {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("test");
        sc = new SparkContext(sparkConf);
        jsc = new JavaSparkContext(sc);
        hiveContext = new HiveContext(jsc);
        spark = new SparkPersistence(hiveContext);
        streamingContext = new JavaStreamingContext(jsc, new Duration(1000));
    }


    @Test
    public void testWrite() {
        String path = "data/person";

        JavaRDD<PersonV1> personRdd1 = getTestRddV1(jsc, 1);
        JavaRDD<PersonV1> personRdd2 = getTestRddV1(jsc, 2);
//
//        spark.write(personRdd1, path, "archive");
//        spark.write(personRdd2, path, "latest");

        Object[] expecteds1 = personRdd1.collect().toArray();
        Object[] expecteds2 = personRdd2.collect().toArray();
        Object[] actuals1 = spark.read(path, "archive", PersonV1.class).collect().toArray();
        Object[] actuals2 = spark.read(path, "latest", PersonV1.class).collect().toArray();

        Assert.assertArrayEquals(expecteds1, actuals1);
        Assert.assertArrayEquals(expecteds2, actuals2);

        Object[] expecteds12 = personRdd2.union(personRdd1).collect().toArray();
        Object[] actuals12 = spark.read(path, null, PersonV1.class).collect().toArray();
        Assert.assertArrayEquals(expecteds12, actuals12);
    }

    @Test
    public void testHive() {
        long timeMillis = System.currentTimeMillis();
        this.hiveContext.sql("drop table if exists person");
        this.hiveContext.sql(
                "create external table person (id string, age int) " +
                        "PARTITIONED BY(name string)" +
                        "CLUSTERED BY(id) INTO 60 BUCKETS " +
                        "STORED AS ORC " +
                        "LOCATION 'file:///Users/ramseytw/projects/sandbox/spark-java/data/person_"+timeMillis+"'");


        this.hiveContext.sql("show tables").show();

        JavaRDD<PersonV2> rdd = getTestRddV2(jsc, 0);
        DataFrame dataFrame1 = this.hiveContext.createDataFrame(rdd, PersonV2.class);
        dataFrame1.registerTempTable("temp_df");
        this.hiveContext.sql("set hive.enforce.bucketing = true; INSERT INTO TABLE person SELECT * FROM temp_df");


        DataFrame dataFrame = this.hiveContext.sql("SELECT * FROM person");
        dataFrame.show();

    }

    @Test
    public void testMultipleVersions() {
        JavaRDD<PersonV1> testRddV1 = getTestRddV1(jsc, 1);
        JavaRDD<PersonV2> testRddV2 = getTestRddV2(jsc, 2);
        String dataset_path = "data/"+System.currentTimeMillis()+"/person";

        spark.write(testRddV1, dataset_path, PersonV1.class);
        spark.write(testRddV2, dataset_path, PersonV2.class);

        JavaRDD<PersonV1> readRdd = spark.read(dataset_path, null, PersonV1.class);
        List<PersonV1> collect = readRdd.collect();
        assertEquals(testRddV1.count() + testRddV2.count(), collect.size());

    }

    @Test
    public void testStreaming() throws InterruptedException {
        Queue<JavaRDD<PersonV2>> inputList = new LinkedBlockingQueue<>();
        List<PersonV2> outputList = new LinkedList<>();

        inputList.add(elementsToRdd(new PersonV2(10, 123, "foo", "GBP")));
        JavaDStream<PersonV2> inputDStream = streamingContext.queueStream(inputList);
        JavaDStream<PersonV2> outputDStream = SparkTransformer.setAgeTo100(inputDStream);

        outputDStream.foreachRDD(new VoidFunction<JavaRDD<PersonV2>>() {
            @Override
            public void call(JavaRDD<PersonV2> personV2JavaRDD) throws Exception {
                System.out.println("*******************");
                System.out.println(personV2JavaRDD.count());
                outputList.addAll(personV2JavaRDD.collect());
            }
        });

        streamingContext.start();
        Thread.sleep(2000);

        PersonV2 p2 = new PersonV2(100, 123, "foo", "GBP");
        System.out.print(outputList);
        assertTrue(outputList.contains(p2));

        inputList.add(elementsToRdd(new PersonV2(10, 456, "foo", "GBP")));
        Thread.sleep(2000);

        PersonV2 p3 = new PersonV2(100, 456, "foo", "GBP");

        // !! cannot add to inputList after streamingContext.start()
        assertFalse(outputList.contains(p3));
    }

    @Test
    public void testConcurrentDebug() {
        String path = "data/*/concurrent_write";
        DataFrame dataFrame = spark.readDataframe(path);
        long count = dataFrame.count();
        System.out.println("***************************");
        System.out.println(count);
    }

    @Test
    public void testConcurrentWrites() {
        String path = "data/concurrent_write";
        long batch = 10000000L;
        DataFrame df1 = this.hiveContext.range(0, batch).coalesce(1).cache();
        DataFrame df2 = this.hiveContext.range(0, Math.round(1.01*batch)).coalesce(1).cache();

        long count1 = df1.count();
        long count2 = df2.count();

        Callable write1 = new Callable() {
            @Override
            public Object call() throws Exception {
                spark.writeDataframe(df1, path);
                return null;
            }
        };

        Callable write2 = new Callable() {
            @Override
            public Object call() throws Exception {
                spark.writeDataframe(df2, path);
                return null;
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(write1);
        executorService.submit(write2);

        long start = System.currentTimeMillis();
        long timoutMillis = 20 * 1000;
        while (true) {//(start + timoutMillis < System.currentTimeMillis()) {
            try {
                DataFrame readDf = spark.readDataframe(path);
                long count = readDf.count();

                System.out.println("##########################");
                System.out.println((System.currentTimeMillis() - start)/1000);
                System.out.println(count);

                if (count == (count1+count2)) {
                    System.out.println("********* READ SUCCESSFULLY ***********");
                    System.out.println((System.currentTimeMillis() - start)/1000);
                    return;
                }
            } catch (Exception e) {}
        }
//        throw new TestFailedException("timeout", 1);

    }

    private <T> JavaRDD<T> elementsToRdd(T... elems) {
        return jsc.parallelize(Arrays.asList(elems));
    }

    public static JavaRDD<PersonV1> getTestRddV1(JavaSparkContext jsc, int start_num) {
        List<PersonV1> personList = Arrays.asList(
                new PersonV1("foo", start_num),
                new PersonV1("foo", 20),
                new PersonV1("baz", 30),
                new PersonV1("baz", 40)
        );

        return jsc.parallelize(personList);
    }

    public static JavaRDD<PersonV2> getTestRddV2(JavaSparkContext jsc, int start_num) {
        List<PersonV2> personList = Arrays.asList(
                new PersonV2(start_num, start_num+1, "foo", "SGP"),
                new PersonV2(20, 21, "bar", "IDN"),
                new PersonV2(30, 31, "baz", "SGP"),
                new PersonV2(40, 41, "qux", "USA")
        );

        return jsc.parallelize(personList);
    }
}