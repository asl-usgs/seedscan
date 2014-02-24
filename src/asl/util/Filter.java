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
package asl.util;

import java.util.Hashtable;
import java.util.Set;

public class Filter
{
    private Hashtable<String, Boolean> filters;
    private boolean exclusive = false;

    public Filter(boolean exclusive)
    {
        this.exclusive = exclusive;
        filters = new Hashtable<String, Boolean>();
    }

    public void addFilter(String key)
    {
        filters.put(key, exclusive);
    }

    public void removeFilter(String key)
    {
        filters.remove(key);
    }

    public boolean filter(String key)
    {
        return filters.containsKey(key) ^ exclusive;
    }
    public Set<String> getKeys()
    {
    	return filters.keySet();
    }
}
