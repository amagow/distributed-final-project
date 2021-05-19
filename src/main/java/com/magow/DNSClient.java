package com.magow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DNSClient {
    private final static Integer numClients = 10;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final APICallType callType = APICallType.addRecord;
    DistributedInterface serverProvider;
    File logFile;

    public DNSClient() {
        try {
            serverProvider = (DistributedInterface) Naming.lookup("Server.DNSServer");
            String path = "logfile/logfile" + "-" + numClients + "-" + callType + "." + "txt";
            logFile = new File(path);
            createLogFile();
        } catch (NotBoundException | IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DNSClient client = new DNSClient();
        client.start();
    }

    private void start() {
        for (int i = 1; i <= numClients; i++) {
            new Thread(new APICaller(i, callType)).start();
        }
    }

    private void createLogFile() throws IOException {
        if (!logFile.createNewFile()) {
            Writer fileWriter = new FileWriter(logFile.getPath(), false);
            fileWriter.write("");
        }
    }

    private enum APICallType {
        query,
        addRecord,
    }

    private class APICaller implements Runnable {
        private final long id;
        private final APICallType callType;

        public APICaller(long id, APICallType callType) {
            this.id = id;
            this.callType = callType;
        }

        @Override
        public void run() {
            try {
                long apiCount = 1;
                Writer fileWriter = new FileWriter(logFile.getPath(), true);

                while (true) {
                    Date startTime = new Date();
                    logInvoke(fileWriter, startTime, apiCount);

                    boolean isSuccessful = false;
                    if (callType == APICallType.addRecord) {
                        isSuccessful = serverProvider.addRecord("test" + id, "test" + id);
                    } else if (callType == APICallType.query) {
                        String query = serverProvider.query("test" + id);
                        isSuccessful = query != null;
                    }

                    Date endTime = new Date();
                    if (isSuccessful) {
                        logSuccess(fileWriter, endTime, apiCount,
                                getDateDiff(startTime, endTime, TimeUnit.MILLISECONDS));
                    } else {
                        warnFail(fileWriter, endTime, apiCount);
                    }

                    apiCount++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
            long diff = date2.getTime() - date1.getTime();
            return timeUnit.convert(diff, TimeUnit.MILLISECONDS);
        }

        private void logInvoke(Writer fileWriter, Date time, long apiCount) throws IOException {
            String log = String.format("%s %s: invoking %s, no.%s, client %s\n"
                    , dateFormat.format(time), "INFO", callType, "" + apiCount, "" + id);
            synchronized (logFile) {
                fileWriter.write(log);
            }
        }

        private void logSuccess(Writer fileWriter, Date time, long apiCount, long latency) throws IOException {
            String log = String.format("%s %s: invoking %s success, no.%s, client %s, latency %dms\n"
                    , dateFormat.format(time), "INFO", callType, "" + apiCount, "" + id, latency);
            synchronized (logFile) {
                fileWriter.write(log);
            }
        }

        private void warnFail(Writer fileWriter, Date time, long apiCount) throws IOException {
            String log = String.format("%s %s: %s failure, no.%s, client %s\n"
                    , dateFormat.format(time), "WARN", callType, "" + apiCount, "" + id);
            synchronized (logFile) {
                fileWriter.write(log);
            }
        }

    }
}