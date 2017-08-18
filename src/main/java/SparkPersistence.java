import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;

public class SparkPersistence {


    private final HiveContext sqlContext;

    public SparkPersistence(HiveContext hiveContext) {

        this.sqlContext = hiveContext;
    }

    public <T> void write(JavaRDD<T> inputRdd, String dataset_path, Class<T> beanClass) {

        Encoder<T> encoder = Encoders.bean(beanClass);
        Dataset<T> dataset = this.sqlContext.createDataset(inputRdd.rdd(), encoder);

        DataFrame dataFrame = dataset.toDF();

        writeDataframe(dataFrame, dataset_path);
    }

    public void writeDataframe(DataFrame dataFrame, String dataset_path) {
        dataFrame.write()
                .format("orc")
//                .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
                .mode(SaveMode.Append)
                .save(dataset_path);
    }

    public <T> JavaRDD<T> read(String path, String suffix, Class<T> beanClass) {
        if (suffix != null) {
            path += "/*/" + suffix;
        }
        DataFrame dataFrame = readDataframe(path);

        Encoder<T> personEncoder = Encoders.bean(beanClass);
        RDD<T> personRDD = dataFrame.as(personEncoder).rdd();

        return personRDD.toJavaRDD();
    }

    public DataFrame readDataframe(String path) {
        return this.sqlContext.read().format("orc").load(path);
    }
}
