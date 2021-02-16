package asl.metadata;

import java.util.Hashtable;

public class StageData {

  private final int stageID;
  private final Hashtable<Integer, Blockette> blockettes;

  // Constructor(s)
  StageData(int stageID) {
    this.stageID = stageID;
    blockettes = new Hashtable<>();
  }

  // stageID
  public int getStageID() {
    return stageID;
  }

  // blockettes
  int addBlockette(Blockette blockette) {
    int blocketteNumber = blockette.getNumber();
    blockettes.put(blocketteNumber, blockette);
    return blocketteNumber;
  }

  public boolean hasBlockette(int blocketteNumber) {
    return blockettes.containsKey(blocketteNumber);
  }

  public Blockette getBlockette(int blocketteNumber) {
    return blockettes.get(blocketteNumber);
  }

  public Hashtable<Integer, Blockette> getBlockettes() {
    return blockettes;
  }
}
