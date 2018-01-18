package asl.freq;

import static org.junit.Assert.*;
import java.util.Arrays;
import org.junit.Test;

public class ButterworthTest {

  @Test
  public void testRolloff() {
    double[] deltaFunction = new double[128];
    for (int i = 0; i < deltaFunction.length; ++i) {
      deltaFunction[i] = Math.sin(i * 2 * Math.PI / 8);
    }
    // deltaFunction[0] = 1;
    SeisGramText sgt = new SeisGramText("");
    ButterworthFilter bf = new ButterworthFilter(sgt, 0, 0.5, 2);
    double[] filtered = bf.apply(1/4., deltaFunction);
    // the filtering process should produce peaks only at the near and far ends of the data
    System.out.println(Arrays.toString(filtered));
    for (int i = 0; i < deltaFunction.length; ++i) {
      if (i == 2 || i == 126) {
        assertTrue(filtered[i] > 0.9);
      } else {
        assertFalse(filtered[i] > 0.9);
      }
    }
  }
  
}
