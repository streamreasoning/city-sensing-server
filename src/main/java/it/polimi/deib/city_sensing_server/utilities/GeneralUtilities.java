package it.polimi.deib.city_sensing_server.utilities;

import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.spi.TimeZoneNameProvider;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

public class GeneralUtilities {
	
	public static String getXsdDateTime(long unixTimestamp) throws DatatypeConfigurationException{
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTimeZone(TimeZone.getTimeZone("Europe/Rome"));
		gc.setTimeInMillis(unixTimestamp);
		XMLGregorianCalendar xmlCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(gc);
		return xmlCalendar.toXMLFormat();
	}

}
