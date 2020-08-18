package com.example.kafka;

import lombok.Data;

import java.util.UUID;

@Data
public class DummyModel {
    private String value1;
    private String value2;

    public static DummyModel instance() {
        DummyModel model = new DummyModel();
        model.value1 = UUID.randomUUID().toString();
        model.value2 = UUID.randomUUID().toString();
        return model;
    }
}
