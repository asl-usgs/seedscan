package asl.timeseries;

import static org.junit.Assert.assertEquals;

import asl.utils.FFTResult;
import org.junit.Test;

public class PreprocessingUtilsTest {


  @Test
  public final void testCostaperNoWidth() throws Exception {
    double[] x = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

    double power = FFTResult.cosineTaper(x, 0);

    assertEquals((Math.round(power)), 16);
    for (double v : x) {
      assertEquals(v, 1, 0);
    }
  }

  @Test
  public final void testCostaperQuarter() throws Exception {
    double[] x = {5, 5, 5, 5, 5};
    double[] answer = {0d, 4.5d, 5d, 4.5d, 0d};

    double power = PreprocessingUtils.costaper(x, 0.25);

    assertEquals((Math.round(power)), (4));
    for (int i = 0; i < x.length; i++) {
      assertEquals(((double) Math.round(x[i] * 10d) / 10d), answer[i], 0);
    }
  }

  @Test
  public final void testRotate45() throws Exception {
    double az1 = 45;
    double az2 = 135;
    double[] x = {1, 1, 1, 1, 1, 1, 1};
    double[] y = {0, 0, 0, 0, 0, 0, 0};
    double[] n = {0, 0, 0, 0, 0, 0, 0};
    double[] e = {0, 0, 0, 0, 0, 0, 0};

    double expected = Math.round(0.70710678118 * 10000000d) / 10000000d; // sin(45 deg)

    PreprocessingUtils.rotate_xy_to_ne(az1, az2, x, y, n, e);
    for (int i = 0; i < n.length; i++) {
      assertEquals(((double) Math.round(n[i] * 10000000d) / 10000000d), expected, 0);
      assertEquals(((double) Math.round(e[i] * 10000000d) / 10000000d), expected, 0);
    }
  }

  @Test
  public final void testRotate330() throws Exception {
    double az1 = 330;
    double az2 = 60;
    double[] x = {1, 1, 1, 1, 1, 1, 1};
    double[] y = {1, 1, 1, 1, 1, 1, 1};
    double[] n = {0, 0, 0, 0, 0, 0, 0};
    double[] e = {0, 0, 0, 0, 0, 0, 0};

    double expectedN =
        Math.round((0.5 + 0.86602540378) * 10000000d) / 10000000d;
    // sin(60) + cos(60)

    double expectedE =
        Math.round((-0.5 + 0.86602540378) * 10000000d) / 10000000d;
    // sin(330) + cos(330)

    PreprocessingUtils.rotate_xy_to_ne(az1, az2, x, y, n, e);
    for (int i = 0; i < n.length; i++) {
      assertEquals(((double) Math.round(n[i] * 10000000d) / 10000000d), expectedN, 0);
      assertEquals(((double) Math.round(e[i] * 10000000d) / 10000000d), expectedE, 0);
    }
  }

  @Test(expected = TimeseriesException.class)
  public final void testRotateHighAzimuthXException() throws Exception {
    double az1 = 361;
    double az2 = 291;
    double[] x = {1, 1, 1, 1, 1, 1, 1};
    double[] y = {1, 1, 1, 1, 1, 1, 1};
    double[] n = {0, 0, 0, 0, 0, 0, 0};
    double[] e = {0, 0, 0, 0, 0, 0, 0};
    PreprocessingUtils.rotate_xy_to_ne(az1, az2, x, y, n, e);
  }

  @Test(expected = TimeseriesException.class)
  public final void testRotateLowAzimuthXException() throws Exception {
    double az1 = 361;
    double az2 = 291;
    double[] x = {1, 1, 1, 1, 1, 1, 1};
    double[] y = {1, 1, 1, 1, 1, 1, 1};
    double[] n = {0, 0, 0, 0, 0, 0, 0};
    double[] e = {0, 0, 0, 0, 0, 0, 0};
    PreprocessingUtils.rotate_xy_to_ne(az1, az2, x, y, n, e);
  }

  @Test(expected = TimeseriesException.class)
  public final void testRotateHighAzimuthYException() throws Exception {
    double az1 = 271;
    double az2 = 361;
    double[] x = {1, 1, 1, 1, 1, 1, 1};
    double[] y = {1, 1, 1, 1, 1, 1, 1};
    double[] n = {0, 0, 0, 0, 0, 0, 0};
    double[] e = {0, 0, 0, 0, 0, 0, 0};
    PreprocessingUtils.rotate_xy_to_ne(az1, az2, x, y, n, e);
  }

  @Test(expected = TimeseriesException.class)
  public final void testRotateLowAzimuthYException() throws Exception {
    double az1 = 88;
    double az2 = -1;
    double[] x = {1, 1, 1, 1, 1, 1, 1};
    double[] y = {1, 1, 1, 1, 1, 1, 1};
    double[] n = {0, 0, 0, 0, 0, 0, 0};
    double[] e = {0, 0, 0, 0, 0, 0, 0};
    PreprocessingUtils.rotate_xy_to_ne(az1, az2, x, y, n, e);
  }

}
