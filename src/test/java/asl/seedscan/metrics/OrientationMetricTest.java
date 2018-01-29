package asl.seedscan.metrics;

import org.junit.Test;

public class OrientationMetricTest {

  @Test
  public void angleToEventTest() {
    double evtLat = 1;
    double evtLon = 5;
    double staLat = 20;
    double staLon = 35;
    double evtLatRad = Math.toRadians(evtLat);
    double evtLonRad = Math.toRadians(evtLon);
    double staLatRad = Math.toRadians(staLat);
    double staLonRad = Math.toRadians(staLon);
    double deltaLon = staLonRad - evtLonRad;
    double x = Math.cos(staLatRad) * Math.sin(deltaLon);
    double y = Math.cos(evtLatRad) * Math.sin(staLatRad);
    y -= Math.sin(evtLatRad) * Math.cos(staLatRad) * Math.cos(deltaLon);
    double result = Math.toDegrees( Math.atan2(x, y) );
    System.out.println(result);
  }
  
}
