package com.monsanto.data.goldengate;

import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.DsMetaData;

public class TxFactory {
    public Tx createAdapterTx(DsTransaction transaction, DsMetaData metaData, DsConfiguration configuration) {
        return new Tx(transaction, metaData, configuration);
    }
}
