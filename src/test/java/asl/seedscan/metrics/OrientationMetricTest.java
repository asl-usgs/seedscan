package asl.seedscan.metrics;

import static org.junit.Assert.*;
import org.junit.Test;

public class OrientationMetricTest {

  @Test
  public void angleToEventTest() {
    double evtLat = 1;
    double evtLon = 5;
    double staLat = 20;
    double staLon = 35;
    
    double ang = EventComparePWaveOrientation.getAngleToEvent(evtLat, evtLon, staLat, staLon);
    assertEquals(ang, 34.8, 1.0);
  }
  
  
  
}
