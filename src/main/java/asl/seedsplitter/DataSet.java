/*
 * Copyright 2012, United States Geological Survey or
 * third-party contributors as indicated by the @author tags.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/  >.
 *
 */
package asl.seedsplitter;

import java.io.Serializable;

/**
 * @author Joel D. Edwards
 * <p>
 * A subclass of Sequence which pairs seismic station and channel information with the data.
 */
public class DataSet extends Sequence implements Serializable {

  // TODO: this could with almost no issues be replaced with the datablock object from java utils
  //   (especially as that includes both the raw data and the basic SNCL metadata)

  /**
   * Serial version UID
   */
  private static final long serialVersionUID = 1L;
  private String m_network;
  private String m_station;
  private String m_location;
  private String m_channel;

  /**
   * Constructor.
   */
  public DataSet() {
    super();
    m_network = null;
    m_station = null;
    m_location = null;
    m_channel = null;
  }

  /**
   * Sets the station's two character network code.
   *
   * @param network network code
   */
  public void setNetwork(String network) {
    m_network = network;
  }

  /**
   * Sets the 3-5 character station code.
   *
   * @param station station code
   */
  public void setStation(String station) {
    m_station = station;
  }

  /**
   * Sets the channel's two digit location code.
   *
   * @param location location code
   */
  public void setLocation(String location) {
    m_location = location;
  }

  /**
   * Sets the three character channel name.
   *
   * @param channel channel name
   */
  public void setChannel(String channel) {
    m_channel = channel;
  }

  /**
   * Returns the station's two character network code.
   *
   * @return network code
   */
  public String getNetwork() {
    return m_network;
  }

  /**
   * Returns the 3-5 character station code.
   *
   * @return station code
   */
  public String getStation() {
    return m_station;
  }

  /**
   * Returns the channel's two digit location code.
   *
   * @return location code
   */
  public String getLocation() {
    return m_location;
  }

  /**
   * Returns the 3 character channel name.
   *
   * @return channel name
   */
  public String getChannel() {
    return m_channel;
  }

}
