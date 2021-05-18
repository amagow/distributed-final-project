package com.magow;

import javax.naming.NamingException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DNSServer extends UnicastRemoteObject implements DistributedInterface {
    public DNSServer() throws IOException, NamingException {
        new Thread(() -> {
            try {
                initCluster();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public static void main(String[] args) {
        try {
            DNSServer server = new DNSServer();
            System.setSecurityManager(new SecurityManager());
            Naming.rebind("Server.DNSServer", server);
            System.out.println("setup server");
        } catch (IOException | NamingException e) {
            e.printStackTrace();
        }
    }

    private void initCluster() throws IOException, InterruptedException {
        String cmd = "goreman -f Procfile start";
        Runtime runtime = Runtime.getRuntime();
        runtime.exec(cmd);
    }

    @Override
    public boolean addRecord(String key, String value) throws IOException {
        String cmd = String.format("etcdctl put %1$s \"%2$s\"", key, value);
        Runtime runtime = Runtime.getRuntime();
        Process proc = runtime.exec(cmd);

        BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));

        String output;
        while ((output = stdInput.readLine()) != null) {
            if (output.equals("OK")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String query(String key) throws IOException {
        String cmd = String.format("etcdctl get %s", key);
        Runtime runtime = Runtime.getRuntime();
        Process proc = runtime.exec(cmd);

        BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));


        String output;
        String prevOutput = null;
        while ((output = stdInput.readLine()) != null) {
            prevOutput = output;
        }
        return prevOutput;
    }
}
