package asl.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ChannelArray {

  private final ArrayList<Channel> channels;

  /**
   * Create an arbitrary length list of channels from a collection of Channels.
   *
   * @param channelsIn Collection of Channel objects.
   */
  public ChannelArray(Collection<Channel> channelsIn) {
    channels = new ArrayList<>();
    for (Channel channel : channelsIn){
      channels.add(new Channel(channel.getLocation(), channel.getChannel()));
    }
  }

  /**
   * Create triplet of Channels from Location Code and Channel Codes.
   * Created channels will share the location.
   *
   * @param location location code EG 00
   * @param channel1 channel code EG LH1
   * @param channel2 channel code EG LH2
   * @param channel3 channel code EG LHZ
   */
  public ChannelArray(String location, String channel1, String channel2,
      String channel3) {
    channels = new ArrayList<>();
    channels.add(new Channel(location, channel1));
    channels.add(new Channel(location, channel2));
    channels.add(new Channel(location, channel3));
  }

  /**
   * Create Channel Array with a single Channel.
   * Used when testing individual channels for rotation in MetricData
   *
   * @param location location code EG 10
   * @param channel1 channel code EG LHED
   */
  public ChannelArray(String location, String channel1) // For testing
  {
    channels = new ArrayList<>();
    channels.add(new Channel(location, channel1));
  }

  /**
   * Create pair of channels from Channel objects.
   * Location may differ between Channels
   *
   * @param channelA first Channel
   * @param channelB second Channel
   */
  public ChannelArray(Channel channelA, Channel channelB) {
    channels = new ArrayList<>();
    channels.add(channelA);
    channels.add(channelB);
  }

  /**
   * @return unmodifiable list of Channels
   */
  public List<Channel> getChannels() {
    return Collections.unmodifiableList(channels);
  }

}
