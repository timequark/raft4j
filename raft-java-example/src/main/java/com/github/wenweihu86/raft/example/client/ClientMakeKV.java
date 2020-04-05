package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.ExampleMessage;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.github.wenweihu86.rpc.client.loadbalance.RandomStrategy;
import com.google.protobuf.util.JsonFormat;

import java.util.Random;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ClientMakeKV {

    public static void main(String[] args) {
        Random random = new Random(System.currentTimeMillis());

        // parse args
        String ipPorts = args[0];
        String key = null;
        String value = null;

        // init rpc client
        RPCClient rpcClient = new RPCClient(ipPorts);
        ExampleService exampleService = RPCProxy.getProxy(rpcClient, ExampleService.class);
        final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

        for(int i = 1, n = 100 * 1024; i <= n; i++) {
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


        rpcClient.stop();
    }
}
