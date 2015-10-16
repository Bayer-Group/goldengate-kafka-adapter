package com.monsanto.data.goldengate.config.factory;

import com.monsanto.data.goldengate.config.Conf;
import com.monsanto.data.goldengate.config.KafkaConfiguration;
import com.monsanto.data.goldengate.config.MetricsConfiguration;
import com.monsanto.data.goldengate.config.TableConfiguration;
import com.monsanto.data.goldengate.encoder.EncoderType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.List;

public class TypesafeConfigFactory implements ConfigurationFactory {
    private final Logger log = LoggerFactory.getLogger(TypesafeConfigFactory.class);
    @Override
    public Conf load(String filePath) {
        log.info("Loading Typesafe config from file="+ filePath);
        Config config = ConfigFactory.parseFile(new File(filePath)).withFallback(ConfigFactory.defaultReference());

        Conf configuration = new Conf(determineEncoderType(config),
                KafkaConfiguration.fromConfig(config.getConfig("kafka")),
                MetricsConfiguration.fromConfig(config.getConfig("metrics")));

        addTableConfigurations(config.getObjectList("tables"), configuration);

        return configuration;
    }

    private void addTableConfigurations(List<? extends ConfigObject> tableConfigs, Conf configuration) {
        for (ConfigObject configObject : tableConfigs) {
            configuration.addTableConfiguration(TableConfiguration.fromConfig(configObject.toConfig()));
        }
    }

    private EncoderType determineEncoderType(Config config) {
        String encodingType = config.getConfig("encoding").getString("type");
        log.info("encoder type="+ encodingType);
        return EncoderType.fromString(encodingType);
    }
}
