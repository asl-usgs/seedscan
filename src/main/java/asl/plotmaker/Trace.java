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

package asl.plotmaker;

import java.awt.Color;
import java.awt.Stroke;

/**
 * @author Mike Hagerty hagertmb@bc.edu
 */
public class Trace {
	private final double[] xdata;
	private final double[] ydata;
	private final String traceName;
	private final Color traceColor;
	private final Stroke stroke;

	// constructor(s)
	public Trace(double[] x, double[] y, String name, Color color, Stroke stroke)
			throws TraceException {
		if (x.length <= 0 || y.length <= 0) {
			throw new TraceException("Either x[] or y[] is empty!");
		}
		if (x.length != y.length) {
			throw new TraceException("x.length != y.length !");
		}
		this.xdata = new double[x.length];
		System.arraycopy(x, 0, this.xdata, 0, x.length);
		this.ydata = new double[y.length];
		System.arraycopy(y, 0, this.ydata, 0, y.length);
		this.traceColor = color;
		this.traceName = name;
		this.stroke = stroke;
	}

	public String getName() {
		return traceName;
	}

	public Color getColor() {
		return traceColor;
	}

	public Stroke getStroke() {
		return stroke;
	}

	public double[] getxData() {
		return xdata;
	}

	public double[] getyData() {
		return ydata;
	}

}
