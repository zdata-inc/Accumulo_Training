package com.minerkasch.accumulo.examples;


import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class BuildGraph {
    private static final String INPUT_FILE = "calls_for_service_10.csv";

    public enum Headers {
        callDateTime, priority, district, description, callNumber, incidentLocation, location
    }


    private static ObjectNode createJsonObj(ArrayList<String> header, CSVRecord record) {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);

        for (int i = 0; i < header.size(); i++) {
            node.put(header.get(i), record.get(i));
        }

        return node;
    }


    public static void main(String[] args) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {

        ClientOnRequiredTable client = new ClientOnRequiredTable();
        client.parseArgs(BuildGraph.class.getName(), args);

        Connector conn = client.getConnector();

        // Create Table if it doesn't already exist
        if (!conn.tableOperations().exists(client.getTableName())) {
            conn.tableOperations().create(client.getTableName());
        }

        // Set the BatchWriter configurations
        long memBuf = 1000000L; // bytes to store before sending a batch
        long timeout = 1000L; // Milliseconds to wait before sending
        int numThreads = 10; // Threads to use to write

        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setTimeout(timeout, TimeUnit.MILLISECONDS);
        writerConfig.setMaxMemory(memBuf);
        writerConfig.setMaxWriteThreads(numThreads);

        BatchWriter writer = conn.createBatchWriter(client.getTableName(), writerConfig);

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        final InputStream resourceAsStream = classLoader.getResourceAsStream(INPUT_FILE);


        System.out.println("Reading from: " + INPUT_FILE);
        try (Reader in = new InputStreamReader(resourceAsStream)) {

            CSVParser parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            Iterator<CSVRecord> records = parser.iterator();


            ArrayList<String> header = new ArrayList<>();

            for (String headerName : parser.getHeaderMap().keySet()) {
                header.add(headerName);
            }


            while (records.hasNext()) {
                CSVRecord record = records.next();

                String priority = record.get(Headers.priority).toLowerCase();
                String district = record.get(Headers.district);
                String description = record.get(Headers.description);

                // Create mappings between priority and district
                writer.addMutation(generateMapping(priority, district, "PRIORITY", "DISTRICT", header, record));
                writer.addMutation(generateMapping(district, priority, "DISTRICT", "PRIORITY", header, record));

                // Create mappings between priority and description
                writer.addMutation(generateMapping(priority, description, "PRIORITY", "DESCRIPTION", header, record));
                writer.addMutation(generateMapping(description, priority, "DESCRIPTION", "PRIORITY", header, record));

                // Create mappings between district and description
                writer.addMutation(generateMapping(district, description, "DISTRICT", "DESCRIPTION", header, record));
                writer.addMutation(generateMapping(description, district, "DESCRIPTION", "DISTRICT", header, record));

            }

        } catch (FileNotFoundException e) {
            System.err.println("File not found");
        } catch (IOException e) {
            System.err.println("Error encountered while reading the file");
        }

        // Close the batch writer
        writer.close();
    }

    private static Mutation generateMapping(String field1, String field2, String headerField1, String headerField2, ArrayList<String> header, CSVRecord record) {

        Mutation m = new Mutation(field1 + "\u0000" + field2);
        m.put(headerField1 + "_" + headerField2, createJsonObj(header, record).toString(), "");
//        String row = field1 + "\u0000" + field2;
//        String cf = headerField1 + "_" + headerField2;
//        String value = createJsonObj(header, record).toString();
//
//
//         System.out.println(row + "\t" + cf + "\t" + value);
         return m;
    }
}
