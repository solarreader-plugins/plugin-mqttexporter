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
package de.schnippsche.solarreader.test;

import de.schnippsche.solarreader.backend.connection.general.ConnectionFactory;
import de.schnippsche.solarreader.backend.connection.mqtt.MqttConnection;
import de.schnippsche.solarreader.database.ExporterData;
import de.schnippsche.solarreader.plugins.mqtt.exporter.MqttExporter;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class MqttExporterTest {
  @Test
  void testMqttExport() throws IOException {
    GeneralTestHelper generalTestHelper = new GeneralTestHelper();
    ExporterData exporterData = new ExporterData();
    exporterData.setName("MqttExporterTest");
    ConnectionFactory<MqttConnection> testFactory = knownConfiguration -> new MqttTestConnection();
    MqttExporter exporter = new MqttExporter(testFactory);
    exporterData.setSetting(exporter.getDefaultExporterSetting());
    exporter.setExporterData(exporterData);
    generalTestHelper.testExporterInterface(exporter);
  }
}
