/*
 * Copyright 2011, United States Geological Survey or
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

/*
 * IllegalSeednameException.java
 *
 * Created on May 24, 2005, 10:04 AM
 *
 *
 */

package seed;

/**
 * @author davidketchum
 */
public class IllegalSeednameException extends Exception {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of IllegalSeednameException
   */
  public IllegalSeednameException(String s) {
    super(s);
  }

}
