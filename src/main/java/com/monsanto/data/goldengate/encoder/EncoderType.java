package com.monsanto.data.goldengate.encoder;

public enum EncoderType {
    JSON;

    public static EncoderType fromString(String str) {
        return Enum.valueOf(EncoderType.class, str.toUpperCase());
    }
}
