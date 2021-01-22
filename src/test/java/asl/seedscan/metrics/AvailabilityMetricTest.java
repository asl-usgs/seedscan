package asl.seedscan.metrics;

import static org.junit.Assert.assertEquals;

import asl.metadata.Channel;
import asl.metadata.ChannelArray;
import asl.metadata.Station;
import asl.metadata.meta_new.StationMeta;
import asl.seedscan.database.MetricDatabaseMock;
import asl.seedscan.database.MetricValueIdentifier;
import asl.testutils.ResourceManager;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.HashMap;
import javax.xml.bind.DatatypeConverter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvailabilityMetricTest {

  private static MetricData maleableData;
  private static MetricData data;
  private static StationMeta metadata;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    data = ResourceManager.loadANMOMainTestCase();
    maleableData = ResourceManager.loadANMOMainTestCase();

    Station metaStation = new Station("CU", "BCIP");
    LocalDate metaDate = LocalDate.ofYearDay(2015, 228);
    String metaLocation = "/metadata/rdseed/CU-BCIP-ascii.txt";
    metadata = ResourceManager.getMetadata(metaLocation, metaDate, metaStation);

    MetricDatabaseMock mockdatabase = new MetricDatabaseMock();

    String expectMetricName = "AvailabilityMetric";
    LocalDate expectDate = LocalDate.parse("2015-07-25");
    Station expectStation = new Station("IU", "ANMO");

    Channel expectChannel = new Channel("10", "BH1"); // Precomputed the digest
    mockdatabase.insertMockDigest(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("9A4FE3A10FD60F93526F464B0DB9580E")));
    expectChannel = new Channel("00", "LH2");
    mockdatabase.insertMockDigest(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        ByteBuffer.wrap("Different".getBytes()));

    maleableData.setMetricReader(mockdatabase);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    maleableData = null;
    data = null;
  }

  @Test
  public final void testGetName() throws Exception {
    Metric metric = new AvailabilityMetric();
    assertEquals("AvailabilityMetric", metric.getName());
  }

  @Test
  public final void testGetVersion() throws Exception {
    Metric metric = new AvailabilityMetric();
    assertEquals(1, metric.getVersion());
  }

  @Test
  public final void testProcess_HasData_NoDB() throws Exception {
    Metric metric = new AvailabilityMetric();
    metric.setData(data);

    /* Should be no HN data in here as it is Triggered*/
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("00,BH1", 100.0);
    expect.put("00,BH2", 100.0);
    expect.put("00,BHZ", 100.0);
    expect.put("00,LH1", 100.0);
    expect.put("00,LH2", 100.0);
    expect.put("00,LHZ", 100.0);
    expect.put("00,VH1", 100.0);
    expect.put("00,VH2", 100.0);
    expect.put("00,VHZ", 100.0);
    expect.put("00,VM1", 100.0);
    expect.put("00,VM2", 100.0);
    expect.put("00,VMZ", 100.0);
    expect.put("10,BH1", 100.0);
    expect.put("10,BH2", 100.0);
    expect.put("10,BHZ", 100.0);
    expect.put("10,LH1", 100.0);
    expect.put("10,LH2", 100.0);
    expect.put("10,LHZ", 100.0);
    expect.put("10,VH1", 100.0);
    expect.put("10,VH2", 100.0);
    expect.put("10,VHZ", 100.0);
    expect.put("10,VMU", 100.0);
    expect.put("10,VMV", 100.0);
    expect.put("10,VMW", 100.0);
    expect.put("20,LN1", 100.0);
    expect.put("20,LN2", 100.0);
    expect.put("20,LNZ", 100.0);
    expect.put("30,LDO", 100.0);
    expect.put("31,LDO", 99.9988425925926);
    expect.put("35,LDO", 99.9988425925926);
    expect.put("40,LF1", 100.0);
    expect.put("40,LF2", 100.0);
    expect.put("40,LFZ", 100.0);
    expect.put("50,LDO", 99.9988425925926);
    expect.put("50,LIO", 99.9988425925926);
    expect.put("50,LKO", 99.9988425925926);
    expect.put("50,LRH", 99.9988425925926);
    expect.put("50,LRI", 99.9988425925926);
    expect.put("50,LWD", 99.9988425925926);
    expect.put("50,LWS", 99.9988425925926);
    expect.put("60,HDF", 100.0);
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_HasData_HasDB() throws Exception {
    MetricDatabaseMock database = new MetricDatabaseMock();

    String metricName = "AvailabilityMetric";
    LocalDate expectDate = LocalDate.parse("2015-07-25");
    Station station = new Station("IU", "ANMO");
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","BH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("92F9D83593E4F53B238772AC1516D223")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","BH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("0C2001E7ED6B3634628474C2DF0DBF0B")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","BHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("9E2B6B87A27AE6A936A6869137C535C5")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("69CFAA3D92F3A02D09956509B3CBE0D1")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("B4AD3642E407DBC7C026D8516A8CE6B5")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("47C4EE2550576DEE885736E38B6D8AF9")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("7704C6294CDC046F8D8BA902111639B4")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C1E80CBB67BC8F058A079BADD007174E")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("B7F5EF3713C201FFCAC158962EBE3C30")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("49FDFD9749EB3CC77BD7865C9B7AEAC7")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("3CFC33FEA35DC590B25EC2D2754B83D4")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C9DAB11A0007215E822490D73C085BA6")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("CE615912E6AE5CC1CDED93ED0D4ECC2D")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("A59F825563BC6D5AF87D33CA0BC57921")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("CE0957C6EF953A38E189A68DE1514C7A")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6097132B87A3799E83F7B2E24712DC61")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("4CA44B8C180B95ACEA3C70AA54230575")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("EA4310A10A07B9271956974B5E1D3924")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LN1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("027D1CA0C127085A8A30D823AEB41851")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LN2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("44FFAE1488D2D3564C2F74398468C72A")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LNZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("2D155BF7A4D4749709201CE908033CCC")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("31","LDO")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("0D8A3B0C5A4F9B052703C2274A45862C")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("35","LDO")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("E8DAF17910873A868908A2CFA3F3DACB")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LDO")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("2A6920EE665BAC33519B2DDA8936E99B")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LIO")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D1E5470085606827725A6A4F0106CAF2")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LKO")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("B8BD7F5C96098186F1F523C883E8F0B4")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LRH")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("CA8ED922B563A84A226F6BB33CA2BD7D")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LRI")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("CA8ED922B563A84A226F6BB33CA2BD7D")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LWD")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("02A7A8AE60B8DA606503F87A0AD17EB0")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LWS")),
        99.9988425925926, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("E0C108F8107A1371A9BFECEF2CBCABFC")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("60","HDF")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("298E8C77D26350C215C54C77AC074360")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VM1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("5CC3259EB1D31B7775C1FA6270820A00")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VM2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("22FE293A395EF3A98167DFBCD77DA3EA")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VMZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D26C89E8FA1BCFFB182C1FED512C4051")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMU")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("59FA283BCB795EC19D3C97164D714CA1")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMV")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("59FA283BCB795EC19D3C97164D714CA1")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMW")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("59FA283BCB795EC19D3C97164D714CA1")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("30","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("850A70BFA1D6E335526BE90DAD8CF143")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LF1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("14940D9DE7129CF90F0A8772A9CA2A83")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LF2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("E5C1E03F1FFCEAD3DE2D2AAE6E01AED5")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LFZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("F2B4B80E3B4265916797F13C484F38B5")));

    Metric metric = new AvailabilityMetric();
    MetricData metricData = ResourceManager.loadANMOMainTestCase();
    metricData.setMetricReader(database);
    metric.setData(metricData);

    HashMap<String, Double> expect = new HashMap<>();
    //All Digests should match so no new results.
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_NoData_NoDB() throws Exception {
    Metric metric = new AvailabilityMetric();
    MetricData metricData = new MetricData(new MetricDatabaseMock(), metadata);
    metric.setData(metricData);
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("20,LNZ", 0.0);
    expect.put("00,VMW", 0.0);
    expect.put("00,BH2", 0.0);
    expect.put("00,VMV", 0.0);
    expect.put("00,BH1", 0.0);
    expect.put("00,VMU", 0.0);
    expect.put("00,LH2", 0.0);
    expect.put("00,LH1", 0.0);
    expect.put("00,BHZ", 0.0);
    expect.put("00,LHZ", 0.0);
    expect.put("20,LN2", 0.0);
    expect.put("20,LN1", 0.0);
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_NoData_HasDB() throws Exception {
    MetricDatabaseMock database = new MetricDatabaseMock();
    LocalDate expectDate = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AvailabilityMetric";
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20", "LNZ")),
        100.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6E04CC6659D82C1DC784A36371BF0335")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "VMW")),
        99.9,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C8280EC927F64731B993EFA825D52929")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "BH2")),
        98.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("828D4BDF7B8194E4F721B6D701C6ED44")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "VMV")),
        40.5,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C8280EC927F64731B993EFA825D52929")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "BH1")),
        35.5,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("828D4BDF7B8194E4F721B6D701C6ED44")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "VMU")),
        10.23,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C8280EC927F64731B993EFA825D52929")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "LH2")),
        0.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("346999B1DBB719CCD2E117DC95832B83")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "LH1")),
        78.9,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("346999B1DBB719CCD2E117DC95832B83")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "BHZ")),
        5.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("828D4BDF7B8194E4F721B6D701C6ED44")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "LHZ")),
        10.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("346999B1DBB719CCD2E117DC95832B83")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20", "LN2")),
        100.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6E04CC6659D82C1DC784A36371BF0335")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20", "LN1")),
        100.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6E04CC6659D82C1DC784A36371BF0335")));

    Metric metric = new AvailabilityMetric();
    MetricData metricData = new MetricData(database, metadata);
    metric.setData(metricData);

    HashMap<String, Double> expect = new HashMap<>();
    /* Should have no new results. Availability doesn't replace existing values if it has no data.*/
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_NoDerivedChannels_NoTriggered() throws Exception {
    Metric metric = new AvailabilityMetric();
    MetricData metricData = ResourceManager.loadANMOMainTestCase();

    //Rotate and add Derived channels to metricData
    metricData.checkForRotatedChannels(new ChannelArray("00", "LHND", "LHED", "LHZ"));
    metric.setData(metricData);

    /* Should be no Triggered HN or HH channels*/
    /* Should be no Derived channels*/
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("00,BH1", 100.0);
    expect.put("00,BH2", 100.0);
    expect.put("00,BHZ", 100.0);
    expect.put("00,LH1", 100.0);
    expect.put("00,LH2", 100.0);
    expect.put("00,LHZ", 100.0);
    expect.put("00,VH1", 100.0);
    expect.put("00,VH2", 100.0);
    expect.put("00,VHZ", 100.0);
    expect.put("00,VM1", 100.0);
    expect.put("00,VM2", 100.0);
    expect.put("00,VMZ", 100.0);
    expect.put("10,BH1", 100.0);
    expect.put("10,BH2", 100.0);
    expect.put("10,BHZ", 100.0);
    expect.put("10,LH1", 100.0);
    expect.put("10,LH2", 100.0);
    expect.put("10,LHZ", 100.0);
    expect.put("10,VH1", 100.0);
    expect.put("10,VH2", 100.0);
    expect.put("10,VHZ", 100.0);
    expect.put("10,VMU", 100.0);
    expect.put("10,VMV", 100.0);
    expect.put("10,VMW", 100.0);
    expect.put("20,LN1", 100.0);
    expect.put("20,LN2", 100.0);
    expect.put("20,LNZ", 100.0);
    expect.put("30,LDO", 100.0);
    expect.put("31,LDO", 99.9988425925926);
    expect.put("35,LDO", 99.9988425925926);
    expect.put("40,LF1", 100.0);
    expect.put("40,LF2", 100.0);
    expect.put("40,LFZ", 100.0);
    expect.put("50,LDO", 99.9988425925926);
    expect.put("50,LIO", 99.9988425925926);
    expect.put("50,LKO", 99.9988425925926);
    expect.put("50,LRH", 99.9988425925926);
    expect.put("50,LRI", 99.9988425925926);
    expect.put("50,LWD", 99.9988425925926);
    expect.put("50,LWS", 99.9988425925926);
    expect.put("60,HDF", 100.0);
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_ASA1_failing_to_compute_nonzero_value() throws Exception {
    Metric metric = new AvailabilityMetric();
    MetricData metricData = ResourceManager.loadASA1MainTestCase();
    metric.setData(metricData);
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("00,HH1", 99.99998842592592);
    expect.put("00,HH2", 99.99998842592592);
    expect.put("00,HHZ", 99.99998842592592);
    expect.put("00,LH1", 99.9988425925926);
    expect.put("00,LH2", 99.9988425925926);
    expect.put("00,LHZ", 99.9988425925926);
    expect.put("00,VM1", 100.0);
    expect.put("00,VM2", 100.0);
    expect.put("00,VM3", 100.0);
    TestUtils.testMetric(metric, expect);
  }
}
