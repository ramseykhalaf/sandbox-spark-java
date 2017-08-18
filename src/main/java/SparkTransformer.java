import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.HashMap;
import java.util.Map;

public class SparkTransformer {

    public static JavaDStream<PersonV2> setAgeTo100(JavaDStream<PersonV2> input) {
        return input.map((person) -> {
            person.setAge(100);
            return person;
        });
    }

    public static JavaDStream<Map<String,Integer>> countByCountry(JavaDStream<PersonV2> input) {
        return input.map((personV2) -> {
            HashMap<String, Integer> map = new HashMap<>();
            return map;
        });

    }
}
