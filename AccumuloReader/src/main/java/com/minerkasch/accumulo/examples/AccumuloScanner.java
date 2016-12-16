package com.minerkasch.accumulo.examples;


import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public class AccumuloScanner {

    public static void main(String args[]) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {

        ClientOnRequiredTable client = new ClientOnRequiredTable();
        ScannerOpts scannerOpts = new ScannerOpts();
        client.parseArgs(AccumuloScanner.class.getName(), args, scannerOpts);

        Connector conn = client.getConnector();
        Scanner scan = conn.createScanner(client.getTableName(), client.auths);

        for (Map.Entry<Key, Value> record : scan) {
            String output = String.format("%s\t%s:%s\t%s", record.getKey().getRow().toString(),
                    record.getKey().getColumnFamily().toString(), record.getKey().getColumnQualifier().toString(),
                    record.getValue().toString());
            System.out.println(output);
        }

        scan.close();
    }
}
