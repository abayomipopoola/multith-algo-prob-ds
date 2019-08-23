package ai.kudi.fridaytalk.wordcount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class WordCountIntegrationTest {
    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void fromDataSetExecuteWordCountAndReturnWordCounts() throws Exception {
        //given
        final List<String> lines = Arrays.asList(
            "Lorem Ipsum is simply dummy text of the printing and typesetting industry",
            "Lorem Ipsum has been the industry standard dummy text ever since the 1500s",
            "Lorem dummy text for the win");

        //when
        final DataSet<Tuple2<String, Integer>> result = WordCount.startWordCount(env, lines);

        //then
        final List<Tuple2<String, Integer>> collect = result.collect();
        assertThat(collect).containsExactlyInAnyOrder(
            new Tuple2<>("lorem", 3),
            new Tuple2<>("ipsum", 2),
            new Tuple2<>("is", 1),
            new Tuple2<>("since", 1),
            new Tuple2<>("typesetting", 1),
            new Tuple2<>("been", 1),
            new Tuple2<>("text", 3),
            new Tuple2<>("the", 4),
            new Tuple2<>("for", 1),
            new Tuple2<>("standard", 1),
            new Tuple2<>("1500s", 1),
            new Tuple2<>("and", 1),
            new Tuple2<>("dummy", 3),
            new Tuple2<>("of", 1),
            new Tuple2<>("industry", 2),
            new Tuple2<>("simply", 1),
            new Tuple2<>("has", 1),
            new Tuple2<>("printing", 1),
            new Tuple2<>("win", 1),
            new Tuple2<>("ever", 1)
        );
    }
}
