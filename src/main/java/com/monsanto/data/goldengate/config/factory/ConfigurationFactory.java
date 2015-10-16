package com.monsanto.data.goldengate.config.factory;

import com.monsanto.data.goldengate.config.Conf;

public interface ConfigurationFactory {

    Conf load(String filePath);

}
