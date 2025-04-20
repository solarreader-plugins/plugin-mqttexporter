/*
 * Copyright (c) 2024-2025 Stefan Toengi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package de.schnippsche.solarreader.plugins.mqtt.exporter;

import de.schnippsche.solarreader.backend.connection.general.ConnectionFactory;
import de.schnippsche.solarreader.backend.connection.mqtt.MqttConnection;
import de.schnippsche.solarreader.backend.connection.mqtt.MqttConnectionFactory;
import de.schnippsche.solarreader.backend.connection.mqtt.PublishEntry;
import de.schnippsche.solarreader.backend.exporter.AbstractExporter;
import de.schnippsche.solarreader.backend.exporter.TransferData;
import de.schnippsche.solarreader.backend.protocol.KnownProtocol;
import de.schnippsche.solarreader.backend.provider.SupportedInterface;
import de.schnippsche.solarreader.backend.singleton.GlobalUsrStore;
import de.schnippsche.solarreader.backend.util.Setting;
import de.schnippsche.solarreader.database.ProviderData;
import de.schnippsche.solarreader.frontend.ui.*;
import de.schnippsche.solarreader.plugin.PluginMetadata;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.tinylog.Logger;

/**
 * The {@link MqttExporter} class is a specific implementation of the {@link AbstractExporter} for
 * exporting data to an MQTT broker. This class facilitates the communication between the
 * application and an MQTT broker, enabling the publishing of data (e.g., sensor readings, status
 * updates) to predefined MQTT topics.
 *
 * <p>As an extension of {@link AbstractExporter}, this class provides the necessary methods to
 * configure the MQTT connection, publish messages, and handle the exporting of data through the
 * MQTT protocol. It can be used to integrate external systems with the MQTT infrastructure for
 * real-time data monitoring, reporting, or automation.
 *
 * <p>The class can manage the configuration of the MQTT connection, message formatting, and
 * exporting processes, ensuring reliable communication between the application and the MQTT broker.
 */
@PluginMetadata(
    name = "MQTTExporter",
    version = "1.0.1",
    author = "Stefan Töngi",
    url = "https://github.com/solarreader-plugins/plugin-MqttExporter",
    svgImage = "mqttexporter.svg",
    supportedInterfaces = {SupportedInterface.NONE},
    usedProtocol = KnownProtocol.HTTP,
    supports = "")
public class MqttExporter extends AbstractExporter implements MqttCallback {
  private static final String TOPIC_NAME = "topic_name";
  private static final String REQUIRED_ERROR = "mqttexporter.required.error";
  private final ConnectionFactory<MqttConnection> connectionFactory;
  private final BlockingQueue<TransferData> queue;
  private MqttConnection connection;
  private String host;
  private String topic;
  private Thread consumerThread;
  private volatile boolean running = true;

  /**
   * Constructs a new instance of the {@link MqttExporter} class using the default {@link
   * MqttConnectionFactory}. This constructor initializes the MQTT exporter with the default
   * connection factory, allowing the application to export data to an MQTT broker. It sets up the
   * necessary configuration for connecting to the broker and prepares the exporter for publishing
   * messages to MQTT topics.
   */
  public MqttExporter() {
    this(new MqttConnectionFactory());
  }

  /**
   * Constructs a new instance of the {@link MqttExporter} class with a custom {@link
   * ConnectionFactory} for creating and managing MQTT connections.
   *
   * <p>This constructor allows for the use of a custom connection factory to establish and manage
   * the connection to the MQTT broker. The custom factory provides more control over the connection
   * setup and can be used to implement specific connection configurations. Additionally, this
   * constructor initializes a {@link LinkedBlockingQueue} for managing messages to be published to
   * the MQTT broker, ensuring thread-safe handling of messages.
   *
   * @param connectionFactory the {@link ConnectionFactory} used to create and manage MQTT
   *     connections.
   */
  public MqttExporter(ConnectionFactory<MqttConnection> connectionFactory) {
    super();
    this.connectionFactory = connectionFactory;
    this.queue = new LinkedBlockingQueue<>();
  }

  @Override
  public ResourceBundle getPluginResourceBundle() {
    return ResourceBundle.getBundle("mqttexporter", locale);
  }

  /** Initializes the mqtt exporter by starting the consumer thread. */
  @Override
  public void initialize() {
    Logger.debug("initialize mqtt exporter");
    consumerThread = new Thread(this::processQueue);
    consumerThread.setName("MQTTExporterThread");
    consumerThread.start();
  }

  /** Shuts down the mqtt exporter by stopping the consumer thread. */
  @Override
  public void shutdown() {
    running = false;
    consumerThread.interrupt();
    if (connection != null) connection.disconnect();
  }

  /**
   * Adds a new transfer data entry to the export queue.
   *
   * @param transferData the transfer data to add.
   */
  @Override
  public void addExport(TransferData transferData) {

    if (transferData.getVariables().isEmpty()) {
      Logger.debug("no exporting variables, skip export");
      return;
    }
    Logger.debug(
        "add {} entries for broker '{}'",
        transferData.getVariables().size(),
        exporterData.getName());
    exporterData.setLastCall(transferData.getTimestamp());
    queue.add(transferData);
  }

  /**
   * Tests the connection to the exporter with the provided configuration.
   *
   * @param testSetting the configuration setting.
   * @return an empty string if the connection test is successful.
   * @throws IOException if the directory does not exist, is not a directory, or is not writable.
   */
  @Override
  public String testExporterConnection(Setting testSetting) throws IOException {
    try (MqttConnection testConnection = connectionFactory.createConnection(testSetting)) {
      testConnection.checkConnection();
    }
    return "";
  }

  /**
   * Retrieves the exporter dialog for the current locale.
   *
   * @return an optional UIList containing the dialog elements.
   */
  @Override
  public Optional<UIList> getExporterDialog() {
    resourceBundle = ResourceBundle.getBundle("mqttexporter", Locale.getDefault());
    UIList uiList = new UIList();
    uiList.addElement(
        new UITextElementBuilder()
            .withLabel(resourceBundle.getString("mqttexporter.title"))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.PROVIDER_HOST)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("mqttexporter.host.tooltip"))
            .withLabel(resourceBundle.getString("mqttexporter.host.text"))
            .withPlaceholder(resourceBundle.getString("mqttexporter.host.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.PROVIDER_PORT)
            .withType(HtmlInputType.NUMBER)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("mqttexporter.port.tooltip"))
            .withLabel(resourceBundle.getString("mqttexporter.port.text"))
            .withPlaceholder(resourceBundle.getString("mqttexporter.port.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());

    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.OPTIONAL_USER)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(false)
            .withTooltip(resourceBundle.getString("mqttexporter.user.tooltip"))
            .withLabel(resourceBundle.getString("mqttexporter.user.text"))
            .withPlaceholder(resourceBundle.getString("mqttexporter.user.text"))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.OPTIONAL_PASSWORD)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(false)
            .withTooltip(resourceBundle.getString("mqttexporter.password.tooltip"))
            .withLabel(resourceBundle.getString("mqttexporter.password.text"))
            .withPlaceholder(resourceBundle.getString("mqttexporter.password.text"))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(TOPIC_NAME)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("mqttexporter.topicname.tooltip"))
            .withLabel(resourceBundle.getString("mqttexporter.topicname.text"))
            .withPlaceholder(resourceBundle.getString("mqttexporter.topicname.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.USE_SSL)
            .withType(HtmlInputType.CHECKBOX)
            .withColumnWidth(HtmlWidth.HALF)
            .withTooltip(resourceBundle.getString("mqttexporter.use.ssl.tooltip"))
            .withLabel(resourceBundle.getString("mqttexporter.use.ssl.label"))
            .withPlaceholder(resourceBundle.getString("mqttexporter.use.ssl.text"))
            .build());

    return Optional.of(uiList);
  }

  /**
   * Retrieves the default exporter configuration.
   *
   * @return a map containing the default configuration parameters.
   */
  @Override
  public Setting getDefaultExporterSetting() {
    Setting setting = new Setting();
    setting.setProviderHost("localhost");
    setting.setProviderPort(1883);
    setting.setReadTimeoutMilliseconds(5000);
    setting.setKeepAliveSeconds(30);
    setting.setSsl(false);
    setting.setConfigurationValue(TOPIC_NAME, "Solarreader");
    return setting;
  }

  @Override
  public void connectionLost(Throwable cause) {
    Logger.error("connection lost: {}", cause.getMessage());
    try {
      connection.connect();
    } catch (ConnectException e) {
      Logger.error(e.getMessage());
    }
  }

  @Override
  public void messageArrived(String topic, MqttMessage mqttMessage) {
    // Nothing to do
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    // Nothing to do
  }

  /** connect to mqtt server. */
  private void connect() {
    try {
      Logger.debug("try connecting to mqtt server '{}' with topic '{}'", host, topic);
      connection.connect();
      Logger.debug("connected to mqtt server '{}'", host, topic);
    } catch (Exception e) {
      Logger.error(e);
    }
  }

  /**
   * Processes the transfer data entry and exports it to a mqtt client.
   *
   * @param transferData the transfer data to export.
   */
  private void doExport(TransferData transferData) {
    if (exporterData.hasConfigurationChanged()) {
      Logger.debug("updated configuration detected...");
      updateConfiguration();
      exporterData.setConfigurationChanged(false);
    }
    if (transferData.getVariables().isEmpty()) {
      Logger.debug("no exporting variables detected...");
      return;
    }
    Logger.debug(
        "export {} entries to '{}' at host '{}' started...",
        transferData.getVariables().size(),
        exporterData.getName(),
        host);

    long startTime = System.currentTimeMillis();
    List<PublishEntry> entries = new ArrayList<>(transferData.getVariables().size());
    long sourceKey = transferData.getSourceKey();
    // Provider or Automation ?
    String providerName = null;
    boolean isProvider = false;
    Logger.debug("search provider with key {}", sourceKey);
    Optional<ProviderData> optionalProviderData =
        GlobalUsrStore.getInstance().getProviderData(sourceKey);

    if (optionalProviderData.isPresent()) {
      providerName = optionalProviderData.get().getName();
      isProvider = true;
      Logger.debug("found provider, name={}", providerName);
    }
    for (Map.Entry<String, Object> entry : transferData.getVariables().entrySet()) {
      String completeTopic =
          isProvider ? generateValidTopic(topic, providerName, entry.getKey()) : entry.getKey();
      Object val = entry.getValue();
      String value =
          val instanceof BigDecimal ? ((BigDecimal) val).toPlainString() : String.valueOf(val);
      entries.add(new PublishEntry(completeTopic, value));
    }
    connection.publish(entries);
    Logger.debug(
        "export to '{}' finished in {} ms",
        exporterData.getName(),
        (System.currentTimeMillis() - startTime));
  }

  /** Processes the export queue by taking each entry and exporting it. */
  private void processQueue() {
    while (running) {
      try {
        TransferData entry = queue.take();
        doExport(entry);
      } catch (InterruptedException e) {
        if (!running) {
          break; // Exit loop if not running
        }
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Generates a valid MQTT topic by combining a topic prefix, a device name, and a leaf. If the
   * prefix is empty or null, it is omitted from the final topic. The method also sanitizes the
   * input to ensure the topic adheres to MQTT topic rules, replacing invalid characters with
   * underscores.
   *
   * @param prefix The topic prefix, can be null or empty.
   * @param deviceName The name of the device, must not be null or empty.
   * @param leaf The leaf element.
   * @return A valid MQTT topic string.
   */
  private String generateValidTopic(String prefix, String deviceName, String leaf) {
    // Sanitize and trim the prefix, deviceName, and leaf
    prefix = sanitizeAndTrim(prefix);
    deviceName = sanitizeAndTrim(deviceName);
    leaf = sanitizeAndTrim(leaf);

    // Validate that the leaf is not empty (leaf is required)
    if (leaf.isEmpty()) {
      throw new IllegalArgumentException("Leaf must not be null or empty");
    }

    // Use StringJoiner to build the topic, skipping empty segments
    StringJoiner topicJoiner = new StringJoiner("/");
    if (!prefix.isEmpty()) topicJoiner.add(prefix);
    if (!deviceName.isEmpty()) topicJoiner.add(deviceName);
    topicJoiner.add(leaf); // leaf is always added since it's mandatory

    return topicJoiner.toString();
  }

  /**
   * Sanitizes a string by replacing invalid characters and trimming leading/trailing slashes. If
   * the input is null or empty after trimming, returns an empty string.
   *
   * @param input The input string to sanitize.
   * @return A sanitized and trimmed string, or an empty string if the input was null/empty.
   */
  private String sanitizeAndTrim(String input) {
    return (input == null || input.trim().isEmpty())
        ? ""
        : input.replaceAll("^/+", "").replaceAll("/+$", "").replaceAll("[^a-zA-Z0-9/_-]", "_");
  }

  /** Updates the configuration of the exporter based on the exporter data. */
  protected void updateConfiguration() {
    if (connection != null) connection.disconnect();
    Setting setting = exporterData.getSetting();
    host = setting.getProviderHost();
    topic = setting.getConfigurationValueAsString(TOPIC_NAME, "");
    connection = connectionFactory.createConnection(setting);
    connect();
  }
}
