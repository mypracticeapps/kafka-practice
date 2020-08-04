package com.example.kafka.util;

public class ThreadUtil {
    public static void sleep(int milli){
        try {
            Thread.sleep(milli);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
