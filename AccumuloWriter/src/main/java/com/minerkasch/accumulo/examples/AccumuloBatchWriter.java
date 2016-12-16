package com.minerkasch.accumulo.examples;


import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class AccumuloBatchWriter {

    public static final String INPUT_FILE = "calls_for_service_100.csv";

    public enum Headers {
        callDateTime, priority, district, description, callNumber, incidentLocation, location
    }

    public static void main(String[] args) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {

        ClientOnRequiredTable client = new ClientOnRequiredTable();
        client.parseArgs(AccumuloBatchWriter.class.getName(), args);

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
        try(Reader in = new InputStreamReader(resourceAsStream)) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);

            Mutation m;
            for (CSVRecord record : records) {
                String priority = record.get(Headers.priority).toUpperCase();
                String district = record.get(Headers.district);
                String incidentLocation = record.get(Headers.incidentLocation);
                String description = record.get(Headers.description);

                m = new Mutation(priority);
                m.put(district.getBytes(), incidentLocation.getBytes(), description.getBytes());

                writer.addMutation(m);
            }
        } catch (FileNotFoundException e) {
            System.err.println("File not found");
        } catch (IOException e) {
            System.err.println("Error encountered while reading the file");
        }

        // Close the batch writer
        writer.close();
    }

}
