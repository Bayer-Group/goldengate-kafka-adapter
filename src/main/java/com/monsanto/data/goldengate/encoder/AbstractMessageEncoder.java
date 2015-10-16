package com.monsanto.data.goldengate.encoder;

import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.TableName;
import com.google.common.base.Optional;
import com.monsanto.data.goldengate.MessageEncoder;
import com.monsanto.data.goldengate.config.Conf;
import com.monsanto.data.goldengate.config.TableConfiguration;

public abstract class AbstractMessageEncoder implements MessageEncoder {
    private final Conf configuration;

    public AbstractMessageEncoder(Conf configuration) {
        this.configuration = configuration;
    }

    protected Optional<TableConfiguration> getConfigurationForTable(TableName tableName) {
        return configuration.getTableConfiguration(tableName.getSchemaName(),tableName.getShortName());
    }

    abstract public byte[] encode(Tx tx, Op op);

}
