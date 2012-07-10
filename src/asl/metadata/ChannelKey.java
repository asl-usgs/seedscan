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
package asl.metadata;

import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class ChannelKey
extends Key
implements Comparable<ChannelKey>
{
    private static final Logger logger = Logger.getLogger("asl.metadata.ChannelKey");
    private static final int CHANNEL_EPOCH_BLOCKETTE_NUMBER = 52;
    private final String errorMsg = "Error: Location code must be 2 chars (\"--\", \"00\", \"10\", etc.)";

    private String location = null;
    private String name = null;

    // constructor(s)
    public ChannelKey(Blockette blockette)
    throws WrongBlocketteException
    {
        if (blockette.getNumber() != CHANNEL_EPOCH_BLOCKETTE_NUMBER) {
            throw new WrongBlocketteException();
        }
        location = blockette.getFieldValue(3,0);
        name     = blockette.getFieldValue(4,0);

        setLocation(location);
        setChannel(name);
    }

    public ChannelKey(String location, String name)
    {
        setLocation(location);
        setChannel(name);
    }

// Valid locations: "--", "00", "10", "20", ...
//       Should we allow no location to be given here and use default="--" ?

    private void setLocation(String location) {

        //if (location == null) {
           //throw new RuntimeException("Error: No channel location code given");
        //}
        if (location == null || location.equals("") ){
          location = "--";
        }
        else {
          if (location.length() != 2) {
             throw new RuntimeException(errorMsg);
          }
          Pattern pattern  = Pattern.compile("^[0-9][0-9]$");
          Matcher matcher  = pattern.matcher(location);
          if (!matcher.matches() && !location.equals("--") ) {
             throw new RuntimeException(errorMsg);
          }
        }
        this.location = location;

    }

    private void setChannel(String channel) {
        if (channel == null) {
            throw new RuntimeException("channel cannot be null");
        }
        if (channel.length() != 3) { // For now let's just allow channel codes = 3 chars
            throw new RuntimeException("station name is too short");
        }
        this.name = channel;
    }



    // identifiers
    public String getLocation()
    {
        return new String(location);
    }

    public String getName()
    {
        return new String(name);
    }

    // overrides abstract method from Key class
    public String toString()
    {
        return new String(location+ "-" +name);
    }

// Use to Sort ChannelKeys from ---BC0, ---BC1, ..., 10-VHZ, ... 50-LWS, 60-HDF
    @Override public int compareTo( ChannelKey chanKey ) {
      String thisCombo = getLocation() + getName();
      String thatCombo = chanKey.getLocation() + chanKey.getName();
      return thisCombo.compareTo(thatCombo);
   }


}

