package com.siva.standalone;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Created by Sivakumar on 26/4/15.
 */
public class GeoIPTest {
    public static void main(String args[]) throws IOException, GeoIp2Exception {
        String ipAddress = "206.80.19.255";
        File database = new File("C:\\Users\\sivakumaran\\Downloads\\MapReduce\\src\\main\\resources\\GeoLite2-Country.mmdb");
        DatabaseReader reader = new DatabaseReader.Builder(database).build();
        CountryResponse countryResponse = reader.country(InetAddress.getByName(ipAddress));
        System.out.println("Country Code: "+countryResponse.getCountry().getIsoCode());
        System.out.println("Country Name: "+countryResponse.getCountry().getName());
    }
}
