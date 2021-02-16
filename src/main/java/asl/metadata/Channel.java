package asl.metadata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Channel {

  private static final Logger logger = LoggerFactory
      .getLogger(asl.metadata.Channel.class);

  private String location = null;
  private String channel = null;

  /**
   * Instantiates a new channel.
   *
   * @param location the location
   * @param channel the channel
   */
  public Channel(String location, String channel) {
    setLocation(location);
    try {
      setChannel(channel);
    } catch (ChannelException e) {
      logger.error("ChannelException:", e);
    }
  }

  /**
   * Validates the Location Code as being either numbers or -- and 2 characters.
   *
   * Note that this does not validate to SEED Convention. Chapter 8 "Fixed Section of Data Header"<br>
   * Seed Convention allows Upper Case and Numerals with left space justification.
   * We do not permit this in DQA.
   *
   * TODO: Determine what validation is actually required.
   *
   * @param location two character string
   * @return false if invalid
   */
  public static boolean validLocationCode(String location) {
    if (location.length() != 2) {
      return false;
    }
    // Allow locations = {"00", "10", "20", ..., "99" and "--"}
    Pattern pattern = Pattern.compile("^[0-9][0-9]$");
    Matcher matcher = pattern.matcher(location);
    return (matcher.matches() || location.equals("--"));

  }

  /**
   * Valids that the band code follows SEED Conventions in Appendix A
   *
   * @param band 1 character band code
   * @return true if valid Band Code was provided
   */
  public static boolean validBandCode(String band) {
    if (band.length() != 1) {
      return false;
    }
    Pattern pattern = Pattern
        .compile("[FGDCESHBMLVURPTQAO]");
    Matcher matcher = pattern.matcher(band);
    return matcher.matches();
  }

  /**
   * Validates with instrument codes that Seedscan supports. EG Seismometers:
   * H,L,G,M,N, and Other: D,F,I,K,R,W,C,E
   *
   * Note: I (James) am not sure who picked this list, but we should consider
   * just allowing any valid code. Metrics can determine themselves if they
   * work on a given type of instrument.
   *
   * @param instrument 1 character Instrument Code
   * @return true if a valid instrument code false if not
   */
  public static boolean validInstrumentCode(String instrument) {
    if (instrument.length() != 1) {
      return false;
    }
    Pattern pattern = Pattern.compile("[HLGMNDFIKRWCE]");
    Matcher matcher = pattern.matcher(instrument);
    return matcher.matches();
  }

  /**
   * Returns true if the channelFlags indicate a continuous channel.
   * If first char is C, it is continous.
   * If it is only a G or H, then we guess it is continous.
   *
   * @param channelFlags Channel flags used to test for Continous
   * @return true if continuous, false otherwise
   */
  public static boolean isContinousChannel(String channelFlags) {
    return channelFlags.charAt(0) == 'C'
        || channelFlags.equals("G") || channelFlags.equals("H");
  }

  /**
   * Returns true if it is a derived channel
   *
   * Standard channels are exactly 3 characters EG LH1.
   * Derived channels are more EG LHED
   *
   * @return true if derived channel
   */
  public boolean isDerivedChannel() {
    return channel != null && channel.length() != 3;
  }

  /**
   * Checks for valid seismometer orientation code.
   * Doesn't allow ABC or TR.
   *
   * @param orientation 1 character OrientationCode
   * @return true if valid false if invalid
   */
  public static boolean validOrientationCode(String orientation) {
    if (orientation.length() != 1) {
      return false;
    }
    Pattern pattern = Pattern.compile("[123NEZUVW]");
    Matcher matcher = pattern.matcher(orientation);
    return matcher.matches();
  }

  // channel setter method(s)

  private void setLocation(String location) {
    if (location != null) {
      // Not sure how we want to validate since CoherencePBM for instance,
      // calls
      // Metric.createIdentifier --> MetricResult.createChannel --> new
      // Channel ("00-10", ...)
      this.location = location;
    } else {
      this.location = "--"; // If no location given, set location = "--"
      // [Default]
    }
  }

  /**
   * Sets the channel.
   *
   * @param channel the channel code
   * @throws ChannelException thrown if the channel code is under 3 characters or null
   */
  public void setChannel(String channel) throws ChannelException {
    if (channel == null) {
      throw new ChannelException("channel cannot be null");
    }
    // Most channels should be exactly 3-chars long (e.g., LH1), however,
    // derived
    // channels (e.g., LHND) will be 4-chars and maybe/probably there will
    // be others
    // e.g., MetricResult.createChannel ( new Channel("00-10" , "LHND-LHND")
    // ) ...
    if (channel.length() < 3) {
      throw new ChannelException(
          "channel code MUST be at least 3-chars long");
    }
    this.channel = channel;
  }

  /**
   * Returns a Channel that should be orthogonal to the input Channel.
   * This channel may not exist or may not have data associated.
   * If the channel is a combined channel EG LH1-LH1, it returns null.
   *
   * @return orthogonal Channel to this channel OR null if unable to determine one.
   */
  public Channel getHorizontalOrthogonalChannel() {
    if (channel.length() > 4) {
      return null;
    }

    //Channel code always has orientation as 3rd char in code regardless of being derived.
    char[] orthogonalChannel = channel.toCharArray();
    switch (orthogonalChannel[2]) {
      case 'N':
        orthogonalChannel[2] = 'E';
        break;
      case 'E':
        orthogonalChannel[2] = 'N';
        break;
      case '1':
        orthogonalChannel[2] = '2';
        break;
      case '2':
        orthogonalChannel[2] = '1';
        break;
      default:
        return null;
    }

    return new Channel(this.location, new String(orthogonalChannel));
  }

  @Override
  public String toString() {
    return getLocation() + "-" + getChannel();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((channel == null) ? 0 : channel.hashCode());
    result = prime * result + ((location == null) ? 0 : location.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Channel other = (Channel) obj;
    if (channel == null) {
      if (other.channel != null) {
        return false;
      }
    } else if (!channel.equals(other.channel)) {
      return false;
    }
    if (location == null) {
      if (other.location != null) {
        return false;
      }
    } else if (!location.equals(other.location)) {
      return false;
    }
    return true;
  }

  public String getLocation() {
    return location;
  }

  public String getChannel() {
    return channel;
  }
}
