package com.horizon.mqclient.utils;

/**
 * the tool for handling String data
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 14:03
 * @see
 * @since : 1.0.0
 */
public class StringUtils {

    public static boolean isEmpty(String str){
        if(str == null || str.length() ==0){
            return true;
        }
        return false;
    }
}
