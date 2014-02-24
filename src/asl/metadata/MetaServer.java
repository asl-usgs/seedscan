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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.net.URI;

import asl.metadata.MetaGenerator;
import asl.metadata.Station;
import asl.metadata.meta_new.StationMeta;

import java.rmi.Naming;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;

/**
 * MetaServer - Return requested metadata
 *
 * @author Mike Hagerty <hagertmb@bc.edu> 
 *
 */
public class MetaServer
{
    private static final Logger logger = LoggerFactory.getLogger(asl.metadata.MetaServer.class);

    private MetaGenerator metaGen = null;
    private MetaInterface meta;
    private boolean useRemoteMeta = false;

    /**
    * Private constructor to ensure singleton
    */

    // Use Remote MetaGenerator registered to rmi runnning on ipString with name="MetaGen":
    public MetaServer(URI remoteURI)
    {
        String urlString = remoteURI.toString();
        //String urlString = "rmi://" + ipString + "/MetaGen";
        // Should probably try to verify ipString here and exit gracefully if not valid
        logger.info("urlString="+urlString);
        try {
            meta = (MetaInterface)Naming.lookup(urlString);
        }
        catch (Exception e) {
            //System.out.format("== MetaServer: Error=%s\n", e.getMessage() );
            logger.error(e.getMessage());
        }
        useRemoteMeta = true;
    }

    // Empty constructor --> Use local MetaGenerator class to load metadata
    public MetaServer(String datalessDir, Set<String> networkSubset) 
    {
        logger.info("use *Local* MetaGenerator: datalessDir=" + datalessDir);
        try {
            metaGen = MetaGenerator.getInstance();
            //metaGen.loadDataless("/Users/mth/mth/ASLData/dcc/metadata/dataless");
            metaGen.loadDataless(datalessDir, networkSubset);
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void quit(){
        try { // Failure to do this may result in a hung application that doesn't quit
            UnicastRemoteObject.unexportObject(metaGen, true);
        }
        catch(RemoteException e) {
            logger.error("Caught exception:", e.getMessage());
        }
    }

    public StationMeta getStationMeta(Station station, Calendar timestamp){
        logger.debug("getStationMeta Station=" + station);
        StationMeta stnMeta = null;
        try {
            if (useRemoteMeta) {
                stnMeta = meta.getStationMeta(station, timestamp);
            }
            else {
                stnMeta = metaGen.getStationMeta(station, timestamp);
            }
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
        logger.debug("getStationMeta Done");
        return stnMeta;
    }

    public List<Station> getStationList() {
        List<Station> stations=null;
        try {
            if (useRemoteMeta) {
                stations = meta.getStationList();
            }
            else {
                stations = metaGen.getStationList();
            }
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
        return stations;
    }

    public void printStationList() {
        List<Station> stations = getStationList();
        for (Station station : stations) {
            System.out.format("     == MetaGen contains Station:[%s]\n", station );
            logger.info("MetaGen contains Station" + station );
        }

    }

}
