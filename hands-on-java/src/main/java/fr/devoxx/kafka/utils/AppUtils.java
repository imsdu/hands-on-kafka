package fr.devoxx.kafka.utils;

import java.util.UUID;

/**
 * Created by fred on 01/04/2017.
 */
public class AppUtils {


    public static String appID(String base ){

       return  base + UUID.randomUUID().toString();
    }
}
