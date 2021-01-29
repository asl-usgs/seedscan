package asl.timeseries;

public class FloatConverter {

  public static double[] convertFloatsToDoubles(float[] input) {
		if (input == null) {
			return null;
		}
		double[] output = new double[input.length];
		for (int i = 0; i < input.length; i++) {
			output[i] = input[i];
		}
		return output;
	}

	public static float[] convertDoublesToFloats(double[] input) {
		if (input == null) {
			return null;
		}
		float[] output = new float[input.length];
		for (int i = 0; i < input.length; i++) {
			output[i] = (float) input[i];
		}
		return output;
	}
}
