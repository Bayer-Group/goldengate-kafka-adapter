package com.monsanto.data.goldengate;

import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;

public interface MessageEncoder {

    byte[] encode(Tx tx, Op op);

}
