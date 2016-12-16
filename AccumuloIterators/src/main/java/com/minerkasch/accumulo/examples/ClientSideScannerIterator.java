package com.minerkasch.accumulo.examples;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.hadoop.io.Text;

import java.util.Map;

public class ClientSideScannerIterator {

    public static void main(String args[]) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {

        ClientOnRequiredTable client = new ClientOnRequiredTable();
        ScannerOpts scannerOpts = new ScannerOpts();
        client.parseArgs(ClientSideScannerIterator.class.getName(), args, scannerOpts);

        Connector conn = client.getConnector();
//        Scanner cscan = new ClientSideIteratorScanner(conn.createScanner(client.getTableName(), client.auths));
        Scanner cscan = conn.createScanner(client.getTableName(), client.auths);

        // Define the iterator
//        IteratorSetting iter = new IteratorSetting(30, "test", RowEnumeratingIterator.class);
        IteratorSetting iter = new IteratorSetting(30, "test", CustomFilteringIterator.class);

        iter.addOption(CustomFilteringIterator.FILTER_VALUE, "theft");
        // Add the iterator to the scanner
        cscan.addScanIterator(iter);

        for (Map.Entry<Key, Value> record : cscan) {
            String output = String.format("%s\t%s:%s\t%s", record.getKey().getRow().toString(),
                    record.getKey().getColumnFamily().toString(), record.getKey().getColumnQualifier().toString(),
                    record.getValue().toString());
            System.out.println(output);
        }

        cscan.close();
    }
}
