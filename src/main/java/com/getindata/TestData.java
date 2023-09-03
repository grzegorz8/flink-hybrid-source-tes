package com.getindata;

import java.time.Instant;

public class TestData {

    private int id;
    private Instant ts;
    private int userId;
    private int value;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Instant getTs() {
        return ts;
    }

    public void setTs(Instant ts) {
        this.ts = ts;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "TestData{" +
                "id=" + id +
                ", ts=" + ts +
                ", userId=" + userId +
                ", value=" + value +
                '}';
    }
}
