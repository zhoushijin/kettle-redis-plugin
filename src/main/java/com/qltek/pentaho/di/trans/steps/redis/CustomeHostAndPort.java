package com.qltek.pentaho.di.trans.steps.redis;

import redis.clients.jedis.HostAndPort;

public class CustomeHostAndPort extends HostAndPort {

    private String password;

    public CustomeHostAndPort(String host, int port, String password) {
        super(host, port);
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
