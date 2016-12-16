package com.minerkasch.accumulo.examples;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Map;

public class AccumuloClientSideScanner {
    public static void main(String args[]) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {

        ClientOnRequiredTable client = new ClientOnRequiredTable();
        ScannerOpts scannerOpts = new ScannerOpts();
        client.parseArgs(AccumuloClientSideScanner.class.getName(), args, scannerOpts);

        Connector conn = client.getConnector();
        Scanner cscan = new ClientSideIteratorScanner(conn.createScanner(client.getTableName(), client.auths));

        cscan.fetchColumnFamily(new Text("CD"));

        for (Map.Entry<Key, Value> record : cscan) {
            String output = String.format("%s\t%s:%s\t%s", record.getKey().getRow().toString(),
                    record.getKey().getColumnFamily().toString(), record.getKey().getColumnQualifier().toString(),
                    record.getValue().toString());
            System.out.println(output);
        }

        cscan.close();
    }
}
