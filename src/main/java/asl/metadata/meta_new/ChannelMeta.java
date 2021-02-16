package asl.metadata.meta_new;

import asl.metadata.Blockette;
import asl.metadata.Channel;
import asl.metadata.ChannelKey;
import asl.metadata.EpochData;
import asl.metadata.StageData;
import asl.metadata.Station;
import asl.security.MemberDigest;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Hashtable;
import org.apache.commons.math3.complex.Complex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ChannelMeta consists of a series of ResponseStages. Typically there will be
 * 3 ResponseStages, numbered 0, 1 and 2. ResponseStages 0 and 2 will likely
 * contain only gain and frequencyOfgain info (e.g., Blockette B058)
 * ResponseStage 1 will contain this info + specific instrument response (e.g.,
 * PoleZero, Polynomial) info, so that the complete channel response can be
 * obtained by scaling ResponseStage1 response by the gains in ResponseStage1
 * and ResponseStage2.
 * 
 * In the future, we may wish to read in higher Stages (3, 4, ...) that are
 * 'D'igital stages with non-zero coefficients (numerator and denominator), so
 * DigitalStage has been left general in order to be able to read and store
 * these coefficients.
 * 
 * @author Mike Hagerty hagertmb@bc.edu
 */
public class ChannelMeta extends MemberDigest implements Serializable,
		Cloneable {
	private static final Logger logger = LoggerFactory
			.getLogger(asl.metadata.meta_new.ChannelMeta.class);
	private static final long serialVersionUID = 3L;

	private final String name;
	private final String location;
	private String instrumentType = null;
	private String channelFlags = null;
	private final String metaDate; // This will be the readable date for
									// metaTimestamp

	private double sampleRate;
	private double elevation;
	private double dip;
	private double azimuth;
	private double depth;
	private final LocalDateTime metaTimestamp; // This should be same as the
											// stationMeta metaTimestamp
	private boolean dayBreak = false; // This will be set to true if channelMeta
										// changes during requested day
	private final Hashtable<Integer, ResponseStage> stages;

	private final Station station;

	public enum ResponseUnits {
		DISPLACEMENT, VELOCITY, ACCELERATION, SEEDUNITS
	}

	// constructor(s)
	public ChannelMeta(ChannelKey channel, LocalDateTime metaTimestamp,
			Station station){
		// We need to call the super constructor to start the MessageDigest
		super();
		this.name = channel.getName();
		this.location = channel.getLocation();
		this.metaTimestamp = metaTimestamp;
		this.metaDate = (EpochData.epochToDateString(this.metaTimestamp));
		this.station = station;
		stages = new Hashtable<>();
	}

	private ChannelMeta(ChannelKey channel, LocalDateTime metaTimestamp){
		this(channel, metaTimestamp, null);
	}

	private ChannelMeta(String location, String channel, LocalDateTime metaTimestamp){
		this(new ChannelKey(location, channel), metaTimestamp);
	}

	ChannelMeta copy(Channel channel){
		return copy(channel.getChannel());
	}

	private ChannelMeta copy(String name){
		String useName;
		if (name != null) {
			useName = name;
		} else {
			useName = this.getName();
		}
		ChannelMeta copyChan = new ChannelMeta(this.getLocation(), useName,
				this.getTimestamp());
		copyChan.sampleRate = this.sampleRate;
		copyChan.dip = this.dip;
		copyChan.azimuth = this.azimuth;
		copyChan.depth = this.depth;
		copyChan.dayBreak = this.dayBreak;
		copyChan.instrumentType = this.instrumentType;
		copyChan.channelFlags = this.channelFlags;

		for (Integer stageID : this.stages.keySet()) {
			ResponseStage stage = this.getStage(stageID);
			ResponseStage copyStage = stage.copy();
			copyChan.addStage(stageID, copyStage);
		}
		return copyChan;
	}

	/**
	 * Add parts of this channelMeta to its digest
	 */
	public void addDigestMembers() {

		addToDigest(sampleRate);
		addToDigest(getNumberOfStages());
		for (Integer stageID : stages.keySet()) {
			ResponseStage stage = getStage(stageID);
			addToDigest(stage.getStageGain());
			addToDigest(stage.getStageGainFrequency());
			addToDigest(stage.getStageType());
			// Add PoleZero Stage to Digest
			if (stage instanceof PoleZeroStage) {
				PoleZeroStage pz = (PoleZeroStage) stage;
				addToDigest(pz.getNormalization());
				ArrayList<Complex> poles = pz.getPoles();
				for (Complex pole : poles) {
					addToDigest(pole.getReal());
					addToDigest(pole.getImaginary());
				}
				ArrayList<Complex> zeros = pz.getZeros();
				for (Complex zero : zeros) {
					addToDigest(zero.getReal());
					addToDigest(zero.getImaginary());
				}
			}
			// Add Polynomial Stage to Digest
			else if (stage instanceof PolynomialStage) {
				PolynomialStage poly = (PolynomialStage) stage;
				addToDigest(poly.getLowerApproximationBound());
				addToDigest(poly.getUpperApproximationBound());
				addToDigest(poly.getNumberOfCoefficients());
				double[] coeffs = poly.getRealPolynomialCoefficients();
				for (double coeff : coeffs) {
					addToDigest(coeff);
				}
			}
			// Add Digital Stage to Digest
			else if (stage instanceof DigitalStage) {
				DigitalStage dig = (DigitalStage) stage;
				addToDigest(dig.getInputSampleRate());
				addToDigest(dig.getDecimation());
			}
		} // end loop over response stages
	} // end addDigestMembers()

	// setter(s)
	public void setSampleRate(double sampleRate) {
		this.sampleRate = sampleRate;
	}

	public void setDip(double dip) {
		this.dip = dip;
	}

	public void setAzimuth(double azimuth) {
		this.azimuth = azimuth;
	}

	public void setDepth(double depth) {
		this.depth = depth;
	}

	public void setElevation(double elevation) { this.elevation = elevation; }

	public void setInstrumentType(String instrumentType) {
		this.instrumentType = instrumentType;
	}

	public void setChannelFlags(String channelFlags) {
		this.channelFlags = channelFlags;
	}

	public void setDayBreak() {
		this.dayBreak = true;
	}

	// getter(s)
	public String getLocation() {
		return location;
	}

	public String getName() {
		return name;
	}

	public double getDepth() {
		return depth;
	}

	public double getDip() {
		return dip;
	}

	public double getElevation() { return elevation; }

	public double getAzimuth() {
		return azimuth;
	}

	public double getSampleRate() {
		return sampleRate;
	}

	public String getInstrumentType() {
		return instrumentType;
	}

	public String getChannelFlags() {
		return channelFlags;
	}

	public LocalDateTime getTimestamp() {
		return metaTimestamp;
	}

	public String getDate() {
		return metaDate;
	}

	// Stages
	private void addStage(Integer stageID, ResponseStage responseStage) {
		stages.put(stageID, responseStage);
	}

	private boolean hasStage(Integer stageID) {
		return stages.containsKey(stageID);
	}

	public ResponseStage getStage(Integer stageID) {
		return stages.get(stageID);
	}

	public Hashtable<Integer, ResponseStage> getStages() {
		return stages;
	}

	// Be careful when using this since:
	// A pole-zero response will have 3 stages: 0, 1, 2
	// A polynomial response will have 1 stage: 1

	public int getNumberOfStages() {
		return stages.size();
	}

	// Return true if any errors found in loaded ResponseStages
	private boolean invalidResponse() {
		// If we have a seismic channel we need to ensure a valid response

		boolean isSeismicChannel = false;
		String seismicCodes = "HN"; // The 2nd char of channels: BH?, LH?, UH?,
									// VH?, EH?, HH?, EN?, LN?, HN?
		if (seismicCodes.contains(this.getName().substring(1, 2))) {
			isSeismicChannel = true;
		}

		/*
		  String excludeCodes = "MDIKRW"; // Channel codes that we DON'T expect
		  to have a stage 0 (e.g., VM?, LD?, LIO, etc.) if
		  (excludeCodes.contains(this.getName().substring(1,2))) {
		  expectChannel0 = false; }
		 */
		if (getNumberOfStages() == 0) {
			logger.error(String
					.format("invalidResponse: No stages have been loaded for chan-loc=%s-%s date=%s\n",
							this.getLocation(), this.getName(), this.getDate()));
			return true;
		}

		if (isSeismicChannel) {
			if (!hasStage(0) || !hasStage(1) || !hasStage(2)) {
				logger.error(String
						.format("invalidResponse: All Stages[=0,1,2] have NOT been loaded for chan-loc=%s-%s date=%s\n",
								this.getLocation(), this.getName(),
								this.getDate()));
				return true;
			}
			double stageGain0 = stages.get(0).getStageGain(); // Really this is
																// the
																// Sensitivity
			double stageGain1 = stages.get(1).getStageGain();
			double stageGain2 = stages.get(2).getStageGain();

			if (stageGain0 <= 0 || stageGain1 <= 0 || stageGain2 <= 0) {
				logger.error(String
						.format("invalidResponse: Gain =0 for either stages 0, 1 or 2 for chan-loc=%s-%s date=%s\n",
								this.getLocation(), this.getName(),
								this.getDate()));
				return true;
			}
			// Check stage1Gain * stage2Gain against the mid-level sensitivity
			// (=stage0Gain):
			double diff = (stageGain0 - (stageGain1 * stageGain2)) / stageGain0;
			diff *= 100;
			// MTH: Adam says that some Q680's have this problem and we should
			// use the Sensitivity (stageGain0) instead:
			if (diff > 10) { // Alert user that difference is > 1% of Sensitivity
				logger.warn(String
						.format("***Alert: stageGain0=%f VS. stage1=%f * stage2=%f (diff=%f%%) date=%s\n",
								stageGain0, stageGain1, stageGain2, diff,
								this.getDate()));
			}

			// MTH: We could also check here that the PoleZero stage(s) was
			// properly loaded
			// But currently this is done when PoleZero.getResponse() is called
			// (it will throw an Exception)
		}

		// If we made it to here then we must have a loaded response

		return false;
	}

	/*
	 * * Return complex PoleZero response computed at given freqs[]* If stage1
	 * != PoleZero stage --> return null
	 */
	public Complex[] getPoleZeroResponse(double[] freqs) {
		PoleZeroStage pz = (PoleZeroStage) this.getStage(1);
		if (pz != null) {
			try {
				return pz.getResponse(freqs);
			} catch (PoleZeroStageException e) {
				logger.error("PoleZeroStageException:", e);
				return null;
			}
		} else {
			return null;
		}
	}

	// Return complex response computed at given freqs[0,...length]

	public Complex[] getResponse(double[] freqs, ResponseUnits responseOut)
			throws ChannelMetaException {
		Complex[] response = getResponseUnscaled(freqs, responseOut);
		// Scale polezero response by stage1Gain * stage2Gain:
		// Unless stage1Gain*stage2Gain is different from stage0Gain
		// (=Sensitivity) by more than 10%,
		// in which case, use the Sensitivity (Adam says this is a problem with
		// Q680's, e.g., IC_ENH
		double stage0Gain = stages.get(0).getStageGain();
		double stage1Gain = stages.get(1).getStageGain();
		double stage2Gain = stages.get(2).getStageGain();

		// Check stage1Gain * stage2Gain against the mid-level sensitivity
		// (=stage0Gain):
		double diff = 100 * (stage0Gain - (stage1Gain * stage2Gain))
				/ stage0Gain;

		double scale;
		if (diff > 10) {
      logger.warn("== getResponse WARNING: Sensitivity != Stage1Gain * Stage2Gain "
					+ "--> Use Sensitivity to scale!");
			scale = stage0Gain;
		} else {
			scale = stage1Gain * stage2Gain;
		}
		for (int i = 0; i < freqs.length; i++) {
			response[i] = response[i].multiply(scale);
		}
		return response;
	}

	public Complex[] getResponseUnscaled(double[] freqs, ResponseUnits responseOut)
			throws ChannelMetaException {
		int outUnits = 0;
		switch (responseOut) {
		case DISPLACEMENT: // return Displacement Response
			outUnits = 1;
			break;
		case VELOCITY: // return Velocity Response
			outUnits = 2;
			break;
		case ACCELERATION: // return Acceleration Response
			outUnits = 3;
			break;
		case SEEDUNITS: // return Default Dataless SEED units response
			outUnits = 0;
			break;
		}

		if (freqs.length == 0) {
			throw new ChannelMetaException("getResponse: freqs.length = 0!");
		}
		if (invalidResponse()) {
			throw new ChannelMetaException("getResponse: Invalid Response!");
		}
		Complex[] response;

		// Set response = polezero response (with A0 factored in):
		ResponseStage stage = stages.get(1);

		if (!(stage instanceof PoleZeroStage)) {
			throw new ChannelMetaException(
					"getResponse: Stage1 is NOT a PoleZeroStage!");
		} else {
			PoleZeroStage pz = (PoleZeroStage) stage;
			try {
				response = pz.getResponse(freqs);
			} catch (PoleZeroStageException e) {
				logger.error("PoleZeroStageException:", e);
				throw new ChannelMetaException("PoleZeroStageException");
			}
			/*Default response (in SEED Units) requested = 0 --> Don't integrate or differentiate */
			if (outUnits != 0) { // Convert response to desired responseOut Units
				int inUnits = stage.getInputUnits(); // e.g., 0=Unknown ;
														// 1=Disp(m) ;
														// 2=Vel(m/s^2) ; 3=Acc
														// ; ...
				if (inUnits == 0) {
					String msg = String
							.format("getResponse:[%s] date:[%s] Response requested but PoleZero Stage Input Units = Unknown!",
									responseOut, this.getDate());
					throw new ChannelMetaException(msg);
				}
				int n = outUnits - inUnits;

				// We need to convert the returned response if the desired
				// response units != the stored response units:
				// inUnit
				// 1 - Displacement
				// 2 - Velocity
				// 3 - Acceleration
				//
				// In the Four Trans convention used by SEED:
				// x(t) ~ Int[ X(w)e^+iwt ] dw ==> x'(t) ~ Int[ iw * X(w)e^+iwt
				// ] dw ==> FFT[x'(t)] = iw x FFT[x(t)]
				//
				// x(t) = v(t) * i(t)
				// X(w) = V(w) x I(w) --> V(w) = X(w)/I(w) = FFT[u'(t)] = iw x
				// U(w) where U(w) = FFT[u(t)]
				// or U(w) = X(w)/{iw x I(w)}
				// U(w) = X(w)/II(w) where II(w) = iw x I(w) <-- divide by this
				// response to achieve integration
				//
				// So, integration n times: multiply I(w) by (iw)^n
				// differentiation n times: multiply I(w) by (-i/w)^n
				//
				// Ex: if the response units are Velocity (inUnits=2) and we
				// want our output units = Acceleration (outUnits=3), then
				// n = 3 - 2 = 1, and we return I'(w)=I(w)/(iw) = -i/w * I(w)

				// Here we set s to be omega. This doesn't depend on the
				// response type as was previous
				double s = 2. * Math.PI;

				// Here we integrate
				if (n < 0) { // INTEGRATION RESPONSE I(w) x (iw)^n
					for (int i = 0; i < freqs.length; i++) {
						Complex iw = new Complex(0.0, s * freqs[i]);
						for (int j = 1; j < Math.abs(n); j++)
							iw = iw.multiply(iw);
						response[i] = iw.multiply(response[i]);
					}
				}
				// Here we differentiate
				else if (n > 0) { // DIFFERENTIATION RESPONSE I(w) / (iw)^n
					for (int i = 0; i < freqs.length; i++) {
						Complex iw = new Complex(0.0, -1.0 / (s * freqs[i]));
						for (int j = 1; j < Math.abs(n); j++)
							iw = iw.multiply(iw);
						response[i] = iw.multiply(response[i]);
					}
				}
			} // Convert
		} // else

		return response;
	}

	/**
	 * processEpochData Convert EpochData = {@literal Hashtable<StageNumber, StageData>}
	 * for this Channel + Epoch Into a sequence of ResponseStages, one for each
	 * StageNumber For now we're just pulling/saving the first 3 stages
	 * 
	 * For each stageNumber, check for a B058 and if present, grab Gain +
	 * freqOfGain Then, if you see a B054 {@literal --> create a new DigitalStage & add to}
	 * ChannelMeta else, if you see a B053 {@literal --> create a new PoleZeroStage & add}
	 * to ChannelMeta else ...
	 **/
	public void processEpochData(EpochData epochData) {

		for (int stageNumber = 0; stageNumber < 3; stageNumber++) {
			if (epochData.hasStage(stageNumber)) {
				StageData stage = epochData.getStage(stageNumber);
				double Gain = 0;
				double frequencyOfGain = 0;
				// Process Blockette B058:
				if (stage.hasBlockette(58)) {
					Blockette blockette = stage.getBlockette(58);
					Gain = Double.parseDouble(blockette.getFieldValue(4, 0));
					String[] temp = blockette.getFieldValue(5, 0).split(" ");
					frequencyOfGain = Double.parseDouble(temp[0]);
					// if (stageNumber == 0) { // Only stage 0 may consist of
					// solely a B058 block
					// In this case Gain=Sensitivity
					if ((stageNumber != 1) && !(stage.hasBlockette(54))) {
						DigitalStage digitalStage = new DigitalStage(
								stageNumber, 'D', Gain, frequencyOfGain);
						this.addStage(stageNumber, digitalStage);
						if (stageNumber != 0) {
							logger.warn("== Warning: MetaGenerator: [{}_{} {}-{}] date:{} stage:{} has NO Blockette B054",
											station.getNetwork(),
											station.getStation(), location,
											name, this.getDate(), stageNumber);
						}
					}
				}
				// else { // No B058: What we do here depends on the stageNumber
				// and the channel name
				// }

				// Process Blockette B053:
				if (stage.hasBlockette(53)) {
					Blockette blockette = stage.getBlockette(53);
					// blockette.print();
					String TransferFunctionType = blockette.getFieldValue(3, 0);
					String ResponseInUnits = blockette.getFieldValue(5, 0);
					String ResponseOutUnits = blockette.getFieldValue(6, 0);
					double A0Normalization = Double.parseDouble(blockette
							.getFieldValue(7, 0));
					@SuppressWarnings("unused")
					Double frequencyOfNormalization = Double
							.parseDouble(blockette.getFieldValue(8, 0));
					int numberOfZeros = Integer.parseInt(blockette
							.getFieldValue(9, 0));
					int numberOfPoles = Integer.parseInt(blockette
							.getFieldValue(14, 0));
					ArrayList<String> RealPoles = blockette.getFieldValues(15);
					ArrayList<String> ImagPoles = blockette.getFieldValues(16);
					ArrayList<String> RealZeros = blockette.getFieldValues(10);
					ArrayList<String> ImagZeros = blockette.getFieldValues(11);
					char[] respType = TransferFunctionType.toCharArray();

					PoleZeroStage pz = new PoleZeroStage(stageNumber,
							respType[0], Gain, frequencyOfGain);
					pz.setNormalization(A0Normalization);
					pz.setInputUnits(ResponseInUnits);
					pz.setOutputUnits(ResponseOutUnits);

					for (int i = 0; i < numberOfPoles; i++) {
						double pole_re = Double.parseDouble(RealPoles.get(i));
						double pole_im = Double.parseDouble(ImagPoles.get(i));
						Complex pole_complex = new Complex(pole_re, pole_im);
						pz.addPole(pole_complex);
					}
					for (int i = 0; i < numberOfZeros; i++) {
						double zero_re = Double.parseDouble(RealZeros.get(i));
						double zero_im = Double.parseDouble(ImagZeros.get(i));
						Complex zero_complex = new Complex(zero_re, zero_im);
						pz.addZero(zero_complex);
					}

					this.addStage(stageNumber, pz);
				} // end B053

				// Process Blockette B062:

				if (stage.hasBlockette(62)) { // This is a polynomial stage,
												// e.g., ANMO_IU_00_VMZ
					Blockette blockette = stage.getBlockette(62);
					// blockette.print();
					String TransferFunctionType = blockette.getFieldValue(3, 0); // Should
																					// be
																					// "P [Polynomial]"
					String ResponseInUnits = blockette.getFieldValue(5, 0);
					String ResponseOutUnits = blockette.getFieldValue(6, 0);
					double lowerFrequencyBound = Double.parseDouble(blockette
							.getFieldValue(9, 0));
					double upperFrequencyBound = Double.parseDouble(blockette
							.getFieldValue(10, 0));
					double lowerApproximationBound = Double
							.parseDouble(blockette.getFieldValue(11, 0));
					double upperApproximationBound = Double
							.parseDouble(blockette.getFieldValue(12, 0));
					int numberOfCoefficients = Integer.parseInt(blockette
							.getFieldValue(14, 0));
					ArrayList<String> RealCoefficients = blockette
							.getFieldValues(15);
					ArrayList<String> ImagCoefficients = blockette
							.getFieldValues(16);
					char[] respType = TransferFunctionType.toCharArray();

					PolynomialStage polyStage = new PolynomialStage(
							stageNumber, respType[0], Gain, frequencyOfGain);

					polyStage.setInputUnits(ResponseInUnits);
					polyStage.setOutputUnits(ResponseOutUnits);
					polyStage.setLowerFrequencyBound(lowerFrequencyBound);
					polyStage.setUpperFrequencyBound(upperFrequencyBound);
					polyStage
							.setLowerApproximationBound(lowerApproximationBound);
					polyStage
							.setUpperApproximationBound(upperApproximationBound);
					for (int i = 0; i < numberOfCoefficients; i++) {
						double coeff_re = Double.parseDouble(RealCoefficients
								.get(i));
						double coeff_im = Double.parseDouble(ImagCoefficients
								.get(i));
						Complex coefficient = new Complex(coeff_re, coeff_im);
						polyStage.addCoefficient(coefficient);
					}
					this.addStage(stageNumber, polyStage);
				} // end B062

				// Process Blockette B054:

				if (stage.hasBlockette(54)) {
					Blockette blockette = stage.getBlockette(54);
					String ResponseInUnits;
					String ResponseOutUnits;

					String transferFunctionType = blockette.getFieldValue(3, 0);
					char[] stageType = transferFunctionType.toCharArray();
					ResponseInUnits = blockette.getFieldValue(5, 0);
					ResponseOutUnits = blockette.getFieldValue(6, 0);

					/*
					 * Check if it is a digital stage. Throw warning if not, but
					 * still continue.
					 */
					if (stageType[0] != 'D') {
						logger.warn("Non digital stage being processed as digital stage.");
					}
					DigitalStage digitalStage = new DigitalStage(stageNumber, stageType[0], Gain, frequencyOfGain);
					digitalStage.setInputUnits(ResponseInUnits);
					digitalStage.setOutputUnits(ResponseOutUnits);

					this.addStage(stageNumber, digitalStage);
				}

			}
		}

		this.setElevation(epochData.getElevation());
		this.setAzimuth(epochData.getAzimuth());
		this.setDepth(epochData.getDepth());
		this.setDip(epochData.getDip());
		this.setSampleRate(epochData.getSampleRate());
		this.setInstrumentType(epochData.getInstrumentType());
		this.setChannelFlags(epochData.getChannelFlags());

	} // end processEpochData

	@Override
	public String toString() {
		return String.format("%15s%s\t%15s%.2f\t%15s%.2f\n", "Channel:",
				name, "sampleRate:", sampleRate, "Depth:", depth) +
				String.format("%15s%s\t%15s%.2f\t%15s%.2f\n",
						"Location:", location, "Azimuth:", azimuth, "Dip:", dip) +
				String.format("%15s%s", "num of stages:", stages.size());
	}
}
