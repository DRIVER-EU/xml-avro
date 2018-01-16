/**
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
package ly.stealth.xmlavro.simple;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class Avro2XmlConverter {

	public static void avroToXml(File avroFile, File xmlFile, Schema schema) throws IOException {
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroFile, datumReader);

		GenericRecord record = dataFileReader.next();

		Document doc;
		try {
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		} catch (ParserConfigurationException e) {
			dataFileReader.close();
			throw new RuntimeException(e);
		}

		Element el = unwrapElement(record, doc);
		doc.appendChild(el);

		saveDocument(doc, xmlFile);
		dataFileReader.close();
	}

	private static Element unwrapElement(GenericRecord record, Document doc) {
		String name = record.getSchema().getName().toLowerCase();
		Element el = doc.createElement(name);
		
		for(Field f : record.getSchema().getFields()) {
			Object value = record.get(f.name());
			if(value instanceof Utf8 || value instanceof GenericData.EnumSymbol) {
				el.appendChild(doc.createTextNode(value.toString()));
			} 
			else {
				System.out.println(value);
			}
		}
		return el;
	}

	private static void saveDocument(Document doc, File file) {
		try {
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.transform(new DOMSource(doc), new StreamResult(file));
		} catch (TransformerException e) {
			throw new RuntimeException(e);
		}
	}
}
