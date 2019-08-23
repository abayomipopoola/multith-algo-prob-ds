package ai.kudi.fridaytalk.probabilisticds;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.agkn.hll.HLL;
import org.assertj.core.data.Offset;
import org.junit.Test;

import java.util.stream.LongStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class HLLRunningTest {

    @Test
    public void givenHLLAddHugeAMountIfNumberThenReturnEstimatedCardinality(){
        //given
        final long numberOfElements = 100_000_000;
        final long toleratedDifference = 1_000_000; // %1 diff.

        final HashFunction hashFunction = Hashing.murmur3_128();
        // The first param denotes how much space HLL should accommodate, the bigger the param,
        // the bigger the precision. But we also need space efficiency, so we used a small param.
        // space
        // the
        // more efficient
        final HLL hll = new HLL(14,5);

        //when
        LongStream.range(0, numberOfElements).forEach(element -> {
                long hashedValue = hashFunction.newHasher().putLong(element).hash().asLong();
                hll.addRaw(hashedValue);
            }
        );

        //then
        final long cardinality = hll.cardinality();
        System.out.println(cardinality);
        assertThat(cardinality).isCloseTo(numberOfElements, Offset.offset(toleratedDifference));
    }

    @Test
    public void givenTwoHLLsAddHugeAmountOfNumbersThenReturnEstimatedCardinality() {
        //given
        final long numberOfElements = 100_000_000;
        final long toleratedDifference = 1_000_000; // %1 diff.

        final HashFunction hashFunction = Hashing.murmur3_128();
        final HLL firstHll = new HLL(15,5);
        final HLL seconfHll = new HLL(15,5);

        //when
        LongStream.range(0, numberOfElements).forEach(element -> {
                long hashedValue = hashFunction.newHasher().putLong(element).hash().asLong();
                firstHll.addRaw(hashedValue);
            }
        );

        LongStream.range(numberOfElements, numberOfElements * 2).forEach(element -> {
                long hashedValue = hashFunction.newHasher().putLong(element).hash().asLong();
                seconfHll.addRaw(hashedValue);
            }
        );

        //then
        firstHll.union(seconfHll);
        final long cardinality = firstHll.cardinality();
        System.out.println(cardinality);
           assertThat(cardinality).isCloseTo(numberOfElements * 2,
            Offset.offset(toleratedDifference *2));
    }
}
