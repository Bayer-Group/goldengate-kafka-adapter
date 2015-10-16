package com.monsanto.data.goldengate.config;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class TableConfiguration {
    private final Logger log = LoggerFactory.getLogger(TableConfiguration.class);
    private final String name;
    private final String schema;
    private final Collection<String> tokenNames;

    public TableConfiguration(String schema, String name, Collection<String> tokenNames) {
        log.info("TableConfiguration Constructor: name="+name+", schema="+schema+", tokenNames="+ tokenNames);
        this.name = name;
        this.schema = schema;
        this.tokenNames = tokenNames;
    }

    public String getName() {
        return name;
    }

    public String getSchema() {
        return schema;
    }

    public Collection<String> getTokenNames() {
        return tokenNames;
    }

    public static TableConfiguration fromConfig(Config config) {
        return new TableConfiguration(config.getString("schema"), config.getString("name"), config.getStringList("tokens"));
    }
}
