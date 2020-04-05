package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.ExampleMessage;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.google.protobuf.util.JsonFormat;

import java.util.Random;

public class BenchmarkTest {
    private static volatile int totalRequestNum = 0;
    private static final int MAX_REQUST_NUM = 1000;

    public static void main(String[] args) {
        Random random = new Random(System.currentTimeMillis());

        // parse args
        String ipPorts = args[0];
        int threadNum = Integer.valueOf(args[1]);

        Thread[] threads = new Thread[threadNum];

        // init rpc client
        RPCClient rpcClient = new RPCClient(ipPorts);
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Thread(new ThreadTask(rpcClient, "thread" + (i+1)));
            threads[i].start();
        }

        while (true) {
            int lastRequestNum = totalRequestNum;
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
            System.out.println(String.format("\nqps = %s\n", (totalRequestNum - lastRequestNum)));
        }
    }

    public static class ThreadTask implements Runnable {

        private RPCClient rpcClient;
        private ExampleService exampleService;
        private String prefix;
        private String key;
        private String value;
        private JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

        public ThreadTask(RPCClient rpcClient, String prefix) {
            this.rpcClient = rpcClient;
            this.exampleService = RPCProxy.getProxy(rpcClient, ExampleService.class);
            this.prefix = prefix;
        }
/*
        String key = null;
        String value = null;


        ExampleService exampleService = RPCProxy.getProxy(rpcClient, ExampleService.class);
        final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

        for(int i = 1, n = 10 * 1024; i <= n; i++) {
            key = String.format("key-%d", i);
            value = ""+i;
            ExampleMessage.SetRequest setRequest = ExampleMessage.SetRequest.newBuilder()
                    .setKey(key).setValue(value).build();
            ExampleMessage.SetResponse setResponse = exampleService.set(setRequest);
            try {
                System.out.printf("set request, key=%s value=%s response=%s\n",
                        key, value, printer.print(setResponse));
                Thread.sleep(100 + random.nextInt(500));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
*/
        public void run() {
            int currentRequestNum = 0;
            long beginTime = beginTime = System.currentTimeMillis();;
            for(int i = 1, n = 100 * 1024; i <= n; i++) {
                key = String.format("%s-%d", prefix, i);
                value = ""+i;
                ExampleMessage.SetRequest setRequest = ExampleMessage.SetRequest.newBuilder()
                        .setKey(key).setValue(value).build();
                ExampleMessage.SetResponse response = exampleService.set(setRequest);
                try {
                    System.out.printf("set request, key=%s value=%s response=%s\n",
                            key, value, printer.print(response));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                if (response != null) {
                    currentRequestNum++;
                    totalRequestNum++;
                    if (currentRequestNum == BenchmarkTest.MAX_REQUST_NUM) {
                        long endTime = System.currentTimeMillis();
                        float averageTime = ((float) (endTime - beginTime)) % BenchmarkTest.MAX_REQUST_NUM;
                        System.out.println("average elpaseMs = " + averageTime);
                        currentRequestNum = 0;
                        beginTime = System.currentTimeMillis();
                    }
                } else {
                    System.out.println(String.format("ERROR:    Set %s -> %s", key, value));
                }
            }
        }

    }
}
