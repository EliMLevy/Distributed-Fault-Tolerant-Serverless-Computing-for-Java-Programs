package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RequestSender {


    private static String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

    public record RequestResponse (String requestBody, String responseBody) {}

    public static void main(String[] args) {

        final int numRequests = Integer.parseInt(args[0]);
        final String gatewayHost = args[1];
        final int gatewayPort = Integer.parseInt(args[2]);

        List<Future<RequestResponse>> responses = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < numRequests; i++) {
            final String code = validClass.replace("world!", "world! from code version " + i);
            responses.add(executor.submit(new Callable<RequestResponse>() {
                @Override
                public RequestResponse call() throws Exception {
                    return new RequestResponse(code, sendMessage(gatewayHost, gatewayPort, code));
                }
            }));
        }

        try {
            printResponses(responses);
        } catch (Exception e) {
           e.printStackTrace();
        }

        executor.shutdownNow();
        
    }


    private static String sendMessage(String hostName, int hostPort, String code) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .uri(URI.create("http://" + hostName + ":" + hostPort + "/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .build();
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        String resBody = new String(response.body());
        return String.format("Code: %d; Body: %s", response.statusCode(), resBody);
    }

    private static void printResponses(List<Future<RequestResponse>> responses) {
        for (int i = 0; i < responses.size(); i++) {

            try {
                RequestResponse rr;
                rr = responses.get(i).get();
                System.out.println("Request: " + rr.requestBody);
                System.out.println("Response: " + rr.responseBody);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
    
}
