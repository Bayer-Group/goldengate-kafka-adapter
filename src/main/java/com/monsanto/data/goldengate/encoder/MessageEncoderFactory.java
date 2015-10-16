package com.monsanto.data.goldengate.encoder;

import com.monsanto.data.goldengate.config.Conf;
import com.monsanto.data.goldengate.encoder.json.JsonEncoder;

public class MessageEncoderFactory {

    public static AbstractMessageEncoder create(Conf configuration) {
        EncoderType encoderType = configuration.encoderType();

        if (EncoderType.JSON.equals(encoderType)) {
            return new JsonEncoder(configuration);
        }
        else {
            throw new IllegalArgumentException("Unknown message encoder type: " + encoderType);
        }
    }
}
