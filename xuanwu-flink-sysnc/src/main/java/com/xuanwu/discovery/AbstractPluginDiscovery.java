/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xuanwu.discovery;

import com.xuanwu.common.Common;
import com.xuanwu.common.PluginIdentifierInterface;
import com.xuanwu.plugin.Plugin;
import com.xuanwu.utils.ReflectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public abstract class AbstractPluginDiscovery<T> implements PluginDiscovery<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPluginDiscovery.class);
    private final Path pluginDir;

    /**
     * Add jar url to classloader. The different engine should have different logic to add url into
     * their own classloader
     */
    private BiConsumer<ClassLoader, URL> addURLToClassLoader = (classLoader, url) -> {
        if (classLoader instanceof URLClassLoader) {
            ReflectionUtils.invoke(classLoader, "addURL", url);
        } else {
            throw new UnsupportedOperationException("can't support custom load jar");
        }
    };

    protected final ConcurrentHashMap<PluginIdentifier, Optional<URL>> pluginJarPath =
            new ConcurrentHashMap<>(Common.COLLECTION_SIZE);

    public AbstractPluginDiscovery(String pluginSubDir, BiConsumer<ClassLoader, URL> addURLToClassloader) {
        this.pluginDir = Common.connectorJarDir(pluginSubDir);
        this.addURLToClassLoader = addURLToClassloader;
        LOGGER.info("Load {} Plugin from {}", getPluginBaseClass().getSimpleName(), pluginDir);
    }

    public AbstractPluginDiscovery(String pluginSubDir) {
        this.pluginDir = Common.connectorJarDir(pluginSubDir);
        LOGGER.info("Load {} Plugin from {}", getPluginBaseClass().getSimpleName(), pluginDir);
    }

    @Override
    public List<URL> getPluginJarPaths(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
                .map(this::getPluginJarPath)
                .filter(Optional::isPresent)
                .map(Optional::get).distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<T> getAllPlugins(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
            .map(this::createPluginInstance).distinct()
            .collect(Collectors.toList());
    }

    @Override
    public T createPluginInstance(PluginIdentifier pluginIdentifier) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        T pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
        if (pluginInstance != null) {
            LOGGER.info("Load plugin: {} from classpath", pluginIdentifier);
            return pluginInstance;
        }
        Optional<URL> pluginJarPath = getPluginJarPath(pluginIdentifier);
        // if the plugin jar not exist in classpath, will load from plugin dir.
        if (pluginJarPath.isPresent()) {
            try {
                // use current thread classloader to avoid different classloader load same class error.
                this.addURLToClassLoader.accept(classLoader, pluginJarPath.get());
            } catch (Exception e) {
                LOGGER.warn("can't load jar use current thread classloader, use URLClassLoader instead now." +
                        " message: " + e.getMessage());
                classLoader = new URLClassLoader(new URL[]{pluginJarPath.get()}, Thread.currentThread().getContextClassLoader());
            }
            pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
            if (pluginInstance != null) {
                LOGGER.info("Load plugin: {} from path: {} use classloader: {}",
                        pluginIdentifier, pluginJarPath.get(), classLoader.getClass().getName());
                return pluginInstance;
            }
        }
        throw new RuntimeException("Plugin " + pluginIdentifier + " not found.");
    }

    @Nullable
    private T loadPluginInstance(PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(getPluginBaseClass(), classLoader);
        for (T t : serviceLoader) {
            if (t instanceof Plugin) {
                // old api
                Plugin<?> pluginInstance = (Plugin<?>) t;
                if (StringUtils.equalsIgnoreCase(pluginInstance.getPluginName(), pluginIdentifier.getPluginName())) {
                    return (T) pluginInstance;
                }
            } else if (t instanceof PluginIdentifierInterface) {
                // new api
                PluginIdentifierInterface pluginIdentifierInstance = (PluginIdentifierInterface) t;
                if (StringUtils.equalsIgnoreCase(pluginIdentifierInstance.getPluginName(), pluginIdentifier.getPluginName())) {
                    return (T) pluginIdentifierInstance;
                }
            } else {
                throw new UnsupportedOperationException("Plugin instance: " + t + " is not supported.");
            }
        }
        return null;
    }

    /**
     * Get the plugin instance.
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin instance.
     */
    protected Optional<URL> getPluginJarPath(PluginIdentifier pluginIdentifier) {
        return pluginJarPath.computeIfAbsent(pluginIdentifier, this::findPluginJarPath);
    }

    /**
     * Get spark plugin interface.
     *
     * @return plugin base class.
     */
    protected abstract Class<T> getPluginBaseClass();

    /**
     * Find the plugin jar path;
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin jar path.
     */
    private Optional<URL> findPluginJarPath(PluginIdentifier pluginIdentifier) {
        if (PLUGIN_JAR_MAPPING.isEmpty()) {
            return Optional.empty();
        }
        final String engineType = pluginIdentifier.getEngineType().toLowerCase();
        final String pluginType = pluginIdentifier.getPluginType().toLowerCase();
        final String pluginName = pluginIdentifier.getPluginName().toLowerCase();
        if (!PLUGIN_JAR_MAPPING.hasPath(engineType)) {
            return Optional.empty();
        }
        Config engineConfig = PLUGIN_JAR_MAPPING.getConfig(engineType);
        if (!engineConfig.hasPath(pluginType)) {
            return Optional.empty();
        }
        Config typeConfig = engineConfig.getConfig(pluginType);
        Optional<Map.Entry<String, ConfigValue>> optional = typeConfig.entrySet().stream()
            .filter(entry -> StringUtils.equalsIgnoreCase(entry.getKey(), pluginName))
            .findFirst();
        if (!optional.isPresent()) {
            return Optional.empty();
        }
        String pluginJarPrefix = optional.get().getValue().unwrapped().toString();
        File[] targetPluginFiles = pluginDir.toFile().listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".jar") && StringUtils.startsWithIgnoreCase(pathname.getName(), pluginJarPrefix);
            }
        });
        if (ArrayUtils.isEmpty(targetPluginFiles)) {
            return Optional.empty();
        }
        try {
            URL pluginJarPath = targetPluginFiles[0].toURI().toURL();
            LOGGER.info("Discovery plugin jar: {} at: {}", pluginIdentifier.getPluginName(), pluginJarPath);
            return Optional.of(pluginJarPath);
        } catch (MalformedURLException e) {
            LOGGER.warn("Cannot get plugin URL: " + targetPluginFiles[0], e);
            return Optional.empty();
        }
    }
}
