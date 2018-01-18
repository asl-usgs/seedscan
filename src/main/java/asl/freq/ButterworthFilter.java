
// added by HPC to put in a package
//package net.alomax.freq;
// change package
package asl.freq;

import org.apache.commons.math3.complex.Complex;

/* 
 * This file is part of the Anthony Lomax Java Library.
 *
 * Copyright (C) 1999 Anthony Lomax <lomax@faille.unice.fr>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */





public class ButterworthFilter implements FrequencyDomainProcess {

	private SeisGramText localeText;
	public double highFreqCorner;
	public double lowFreqCorner;
	public int numPoles;
	public int filterType;

	public String errorMessage;

	private static final double FREQ_MIN = 1.0e-5;
	private static final double FREQ_MAX = 1.0e5;

	private static final double NUM_POLES_MIN = 2;
	private static final double NUM_POLES_MAX = 20;

	private static final double PI = Math.PI;
	private static final double TWOPI = 2.0 * Math.PI;

        public static final int CAUSAL = 0;
        public static final int NONCAUSAL = 1;
        public static final int TWOPASS = 1;

	/** constructor */

    public ButterworthFilter(SeisGramText localeText, 
                             double lowFreqCorner, 
                             double highFreqCorner, 
			     int numPoles) {
           this(localeText, lowFreqCorner, highFreqCorner, numPoles, CAUSAL);
	   }

    public ButterworthFilter(SeisGramText localeText, 
                             double lowFreqCorner, 
                             double highFreqCorner, 
			     int numPoles,
			     int filterType) {
		this.localeText = localeText;
		this.highFreqCorner = highFreqCorner;
		this.lowFreqCorner = lowFreqCorner;
		this.numPoles = numPoles;
		this.filterType = filterType;
		this.errorMessage = " ";
	}


	/** Method to set high frequency corner */

	public void setHighFreqCorner(double freqValue) 
									throws FilterException {
		if (freqValue < FREQ_MIN || freqValue > FREQ_MAX) {
			throw new FilterException(
				localeText.invalid_high_frequency_corner);
		}

		highFreqCorner = freqValue;
	}


	/** Method to set high frequency corner */

	public void setHighFreqCorner(String str)
									throws FilterException {

		double freqValue;

		try {
			freqValue = Double.valueOf(str).doubleValue();
		} catch (NumberFormatException e) {
			throw new FilterException(
				localeText.invalid_high_frequency_corner);
		}

		setHighFreqCorner(freqValue);
	}


	/** Method to set low frequency corner */

	public void setLowFreqCorner(double freqValue)
									throws FilterException {
		if (freqValue < FREQ_MIN || freqValue > FREQ_MAX) {
			throw new FilterException(
				localeText.invalid_low_frequency_corner);
		}

		lowFreqCorner = freqValue;
	}


	/** Method to set low frequency corner */

	public void setLowFreqCorner(String str)
									throws FilterException {

		double freqValue;

		try {
			freqValue = Double.valueOf(str).doubleValue();
		} catch (NumberFormatException e) {
			throw new FilterException(
				localeText.invalid_low_frequency_corner);
		}

		setLowFreqCorner(freqValue);
	}


	/** Method to set number of poles */

	public void setNumPoles(int nPoles)
									throws FilterException {

		if (nPoles < NUM_POLES_MIN || nPoles > NUM_POLES_MAX
				|| nPoles % 2 != 0) {
			throw new FilterException(
				localeText.invalid_number_of_poles);
		}

		numPoles = nPoles;
	}


	/** Method to set number of poles */

	public void setNumPoles(String str)
									throws FilterException {

		int nPoles;

		try {
			nPoles = Integer.parseInt(str);
		} catch (NumberFormatException e) {
			throw new FilterException(
				localeText.invalid_number_of_poles);
		}

		setNumPoles(nPoles);
	}



	/** Method to check settings */

	void checkSettings() throws FilterException {

		String errMessage = "";
		int badSettings = 0;

		if (highFreqCorner < FREQ_MIN || highFreqCorner > FREQ_MAX) {
			errMessage += ": " + localeText.invalid_high_frequency_corner;
			badSettings++;
		}

		if (lowFreqCorner < FREQ_MIN || lowFreqCorner > FREQ_MAX) {
			errMessage += ": " + localeText.invalid_low_frequency_corner;
			badSettings++;
		}

		if (lowFreqCorner >= highFreqCorner) {
			errMessage += 
				": " + localeText.low_corner_greater_than_high_corner;
			badSettings++;
		}

		if (numPoles < NUM_POLES_MIN || numPoles > NUM_POLES_MAX
				|| numPoles % 2 != 0) {
			errMessage += ": " + localeText.invalid_number_of_poles;
			badSettings++;
		}

		if (badSettings > 0) {
			throw new FilterException(errMessage + ".");
		}

	}

	public final double[] apply(double dt, double[] db) {
	  Complex[] cx = new Complex[db.length];
	  for (int i = 0; i < db.length; ++i) {
	    cx[i] = new Complex(db[i]);
	  }
	  Complex[] filtCx = apply(dt, cx);
	  double[] result = new double[filtCx.length];
	  for (int i = 0; i < filtCx.length; ++i) {
	    result[i] = filtCx[i].getReal();
	  }
	  return result;
	}
	

    /**
     * method to do Butterworth band-pass filter in freq domain **
     *
     * bandpass filter (nPBP Butterworth Filter)
     *
     * convolve with nPole Butterworth Bandpass filter
     *
     * where - fl - low frequency corner in Hz fh - high frequency corner in Hz npole - number of poles in filter at each corner (not more than 20)
     * npts - number of complex fourier spectral coefficients dt - sampling interval in seconds cx - complex fourier spectral coefficients
     *
     * @param dt - sampling interval
     * @param cx - data to run filter on
     * @return
     */
    @Override
    public final Complex[] apply(double dt, Complex[] cx) {

        int npts = cx.length;
        double fl = lowFreqCorner;
        double fh = highFreqCorner;
        int npole = numPoles;

        Complex c0 = new Complex(0.0, 0.0);
        Complex c1 = new Complex(1.0, 0.0);

        Complex[] sph = new Complex[numPoles];
        Complex[] spl = new Complex[numPoles];

        Complex cjw, cph, cpl;
        int nop, nepp, np;
        double wch, wcl, ak, ai, ar, w, dw;
        int i, j;

        if (npole % 2 != 0) {
            System.out.println("WARNING - Number of poles not a multiple of 2!");
        }

        nop = npole - 2 * (npole / 2);
        nepp = npole / 2;
        wch = TWOPI * fh;
        wcl = TWOPI * fl;

        np = -1;
        if (nop > 0) {
            np += 1;
            sph[np] = new Complex(1., 0.);
        }
        if (nepp > 0) {
            for (i = 0; i < nepp; i++) {
                ak = 2. * Math.sin((2. * (double) i + 1.0) * PI / (2. * (double) npole));
                ar = ak * wch / 2.;
                ai = wch * Math.sqrt(4. - ak * ak) / 2.;
                np += 1;
                sph[np] = new Complex(-ar, -ai);
                np += 1;
                sph[np] = new Complex(-ar, ai);
            }
        }
        np = -1;
        if (nop > 0) {
            np += 1;
            spl[np] = new Complex(1., 0.);
        }
        if (nepp > 0) {
            for (i = 0; i < nepp; i++) {
                ak = 2. * Math.sin((2. * (double) i + 1.0) * PI / (2. * (double) npole));
                ar = ak * wcl / 2.;
                ai = wcl * Math.sqrt(4. - ak * ak) / 2.;
                np += 1;
                spl[np] = new Complex(-ar, -ai);
                np += 1;
                spl[np] = new Complex(-ar, ai);
            }
        }

        cx[0] = c0;
        dw = TWOPI / ((double) npts * dt);
        w = 0.;
        for (i = 1; i < npts / 2 + 1; i++) {
            w += dw;
            cjw = new Complex(0., -w);
            cph = c1;
            cpl = c1;
            for (j = 0; j < npole; j++) {
              cph = cph.multiply(sph[j].divide(sph[j].add(cjw)));
              cpl = cpl.multiply(cjw.divide(spl[j].add(cjw)));
               // cph = Complex.mul(cph, Complex.div(sph[j], Complex.add(sph[j], cjw)));
               // cpl = Complex.mul(cpl, Complex.div(cjw, Complex.add(spl[j], cjw)));
// Does not work! : following 2 lines to replace preceeding 2 lines
//              cph.mul(Complex.div(sph[j], Complex.add(sph[j], cjw)));
//              cpl.mul(Complex.div(cjw, Complex.add(spl[j], cjw)));
//orig              cph = Complex.div(Complex.mul(cph, sph[j]), Complex.add(sph[j], cjw));
//orig              cpl = Complex.div(Complex.mul(cpl, cjw), Complex.add(spl[j], cjw));
            }
            cx[i] = cx[i].multiply(cph.multiply(cpl).conjugate());
            //cx[i].mul(Complex.mul(cph, cpl).conjg());
//orig          cx[i] = Complex.mul(cx[i], Complex.conjg(Complex.mul(cph, cpl)));
            cx[npts - i] = cx[i].conjugate();
        }

        return (cx);
    
    }
	


}	// End class ButterworthFilter


