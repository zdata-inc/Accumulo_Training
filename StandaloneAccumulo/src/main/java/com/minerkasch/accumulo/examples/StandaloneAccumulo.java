package com.minerkasch.accumulo.examples;

import com.google.common.io.Files;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.shell.Shell;

import java.io.File;
import java.io.IOException;


public class StandaloneAccumulo {
    private static final String PASSWORD = "password";
    private static MiniAccumuloCluster mac;

    public static void main(String args[]) throws IOException, InterruptedException {
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();

        mac = new MiniAccumuloCluster(tempDir, PASSWORD);
        mac.start();

        System.out.println("Minicluster Starting...");

        System.out.println("############################################################");
        System.out.println("Instance Name: " + mac.getInstanceName());
        System.out.println("Root Password: " + PASSWORD);
        System.out.println("ZooKeepers: " + mac.getZooKeepers());
        System.out.println("Temporary directory: " + mac.getConfig().getDir());
        System.out.println("############################################################");

        System.out.println("Starting the shell");
        String[] shellArgs = new String[] {"-u", "root",
                "-p", PASSWORD,
                "-zi", mac.getInstanceName(),
                "-zh", mac.getZooKeepers()};
        Shell shell = new Shell();
        shell.config(shellArgs);
        shell.start();
        shell.shutdown();

        System.out.println("Shutting down MAC");
        mac.stop();
    }
}
