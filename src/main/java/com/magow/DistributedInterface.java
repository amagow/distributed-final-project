package com.magow;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DistributedInterface extends Remote {
    boolean addRecord(String key, String value) throws IOException;

    String query(String key) throws IOException;
}
