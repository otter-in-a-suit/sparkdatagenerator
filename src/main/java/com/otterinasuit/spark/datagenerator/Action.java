package com.otterinasuit.spark.datagenerator;

import java.io.Serializable;
import java.util.Random;

/**
 * Generates random records for this article
 * https://otter-in-a-suit.com/blog/?p=12
 *
 * @author Christian Hollinger (christian.hollinger@otter-in-a-suit.com)
 */
public class Action implements Serializable{
    private int user, action, client;
    private long ts;
    private String key;

    public Action(){
        user = getRandom(0, Integer.MAX_VALUE-1);
        action = getRandom(0, 99);
        // 2016-06-01 - 2016-10-01
        ts = getRandom(1464739200, 1475280000);
        // 1 = Mac; 2 = Windows; 3 = undefined
        // 1471824000 = 2016-08-22 (midnight), 1472083199 = 2016-08-24 (23:59:59)
        client = ((ts >= 1471824000) && (ts <= 1472083199)) ? getRandom(0, 2) : getRandom(1, 3);
        key = getKey();
    }

    public int getUser() {
        return user;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public int getClient() {
        return client;
    }

    public void setClient(int client) {
        this.client = client;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getKey() {
        key = user + "_" + ts;
        return key;
    }

    private static int getRandom(int min, int max){
        return new Random().nextInt(max - min + 1) + min;
    }

    @Override
    public String toString() {
        return "Action{" +
                "user=" + user +
                ", action=" + action +
                ", client=" + client +
                ", ts=" + ts +
                ", key='" + key + '\'' +
                '}';
    }
}
