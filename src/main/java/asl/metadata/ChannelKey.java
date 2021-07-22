package asl.metadata;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelKey extends Key implements Comparable<ChannelKey>, Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory
      .getLogger(asl.metadata.ChannelKey.class);
  private static final int CHANNEL_EPOCH_BLOCKETTE_NUMBER = 52;

  private String location = null;
  private String name = null;

  public ChannelKey(Blockette blockette) throws WrongBlocketteException {
    if (blockette.getNumber() != CHANNEL_EPOCH_BLOCKETTE_NUMBER) {
      throw new WrongBlocketteException();
    }
    location = blockette.getFieldValue(3, 0);
    name = blockette.getFieldValue(4, 0);

    try {
      setChannel(name);
      setLocation(location);
    } catch (ChannelKeyException e) {
      logger.error("Exception:", e);
    }
  }

  public ChannelKey(String location, String name) {
    try {
      setChannel(name);
      setLocation(location);
    } catch (ChannelKeyException e) {
      logger.error("Exception:", e);
    }
  }

  public ChannelKey(Channel channel) {
    this(channel.getLocation(), channel.getChannel());
  }

  /**
   * Sets the location code.
   * <p>
   * If the location code is -- or "" it is changed to 00. If the location code is HR it is changed
   * to 10.
   *
   * @param location valid codes include --, "", HR, 00, 10, etc
   * @throws ChannelKeyException if an invalid location code was provided
   */
  private void setLocation(String location) throws ChannelKeyException {

    String validCodes = "\"--\", \"00\", \"10\", etc.";

    // MTH: Known abnormal location codes:
    // HR - High res instrument, later replaced by 10 - seen in US seed
    // files
    // XX - seen in II seed files
    // P1 - Princeton Modes Synthetic - seen in II seed files
    // P3 - Princeton SEM Synthetic - seen in II seed files

		/*
		   Temp fix for station US_WMOK which has some channel blockettes
		  tagged with location="HR" if (location.equals("HR")) { // Add to this
		  any unruly location code you want to flag ... location = "XX";
		  logger.warn( String.format(
		  "ChannelKey.setLocation: Got location code=HR chan=%s--> I'll set it to XX and continue parsing dataless"
		  , name) ); }
		 */

    // Set Default location codes:
    if (location == null || location.equals("")) {
      logger.debug(
          "metadata name=[{}] location=[{}] was changed to [--]",
          name, location);
      location = "--";
    }


    if (location.length() != 2) {
      throw new ChannelKeyException(String
          .format("Location code=[%s] chan=[%s] is NOT a valid 2-char code (e.g., %s)\n",
              location, name, validCodes));
    }

    this.location = location;
  }

  private void setChannel(String channel) throws ChannelKeyException {
    if (channel == null) {
      // RuntimeException e = new RuntimeException(message.toString());
      throw new ChannelKeyException("Channel cannot be null");
    }
    // MTH: For now we'll allow either 3-char ("LHZ") or 4-char ("LHND")
    // channels
    if (channel.length() < 3 || channel.length() > 4) {
      throw new ChannelKeyException(String
          .format("Channel code=[%s] is NOT valid (must be 3 or 4-chars long)\n",
              channel));
    }
    this.name = channel;
  }

  public Channel toChannel() {
    return new Channel(this.getLocation(), this.getName());
  }

  // identifiers
  public String getLocation() {
    return location;
  }

  public String getName() {
    return name;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return location + "-" + name;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((location == null) ? 0 : location.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  /*
   * (non-Javadoc)
   *
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
    ChannelKey other = (ChannelKey) obj;
    if (location == null) {
      if (other.location != null) {
        return false;
      }
    } else if (!location.equals(other.location)) {
      return false;
    }
    if (name == null) {
      return other.name == null;
    } else {
      return name.equals(other.name);
    }
  }

  /**
   * Comparisons are done by converting location and name to string for both ChannelKeys and
   * returning the String.compareTo results.
   *
   * @param chanKey The channel key we are comparing to.
   */
  @Override
  public int compareTo(ChannelKey chanKey) {
    String thisCombo = getLocation() + getName();
    String thatCombo = chanKey.getLocation() + chanKey.getName();
    return thisCombo.compareTo(thatCombo);
  }
}
