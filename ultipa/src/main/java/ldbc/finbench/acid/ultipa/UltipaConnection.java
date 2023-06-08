package ldbc.finbench.acid.ultipa;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import okhttp3.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class UltipaConnection {
    private String httpServer;
    private String host;
    private int port;
    private String username;
    private String password;
    private OkHttpClient okHttpClient;

    private String transactionId;

    static final ConnectionPool CONNECTION_POOL = new ConnectionPool(20, 600, TimeUnit.SECONDS);

    public UltipaConnection(String httpServer, String host, int port, String username, String password) {
        Objects.requireNonNull(httpServer);
        this.httpServer = httpServer.endsWith("/") ? httpServer.substring(0, httpServer.length() - 1) : httpServer;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;

    }

    public static void assertSuccess(okhttp3.Response response) {
        if (!response.isSuccessful()) {
            throw new RuntimeException("http request failed: " + response);
        }
    }

    public void connect() throws IOException {
        okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(1000, TimeUnit.SECONDS)
                .readTimeout(1000, TimeUnit.SECONDS)
                .connectionPool(CONNECTION_POOL)
                .build();

        {
            Gson gson = new Gson();
            String json = gson.toJson(ImmutableMap.of("ip", host, "port", port, "username", username, "password", password));
            RequestBody requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);

            Request request = new Request.Builder()
                    .url(httpServer + "/login")
                    .post(requestBody)
                    .build();

            try (okhttp3.Response response = okHttpClient.newCall(request).execute()) {
                assertSuccess(response);
            }

        }
    }

    public void disconnect() throws IOException {
        if (okHttpClient != null) {
            RequestBody requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), "{}");

            Request request = new Request.Builder()
                    .url(httpServer + "/logout")
                    .post(requestBody)
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                assertSuccess(response);
            }
        }
    }

    public void begin() throws IOException {

        {
            Gson gson = new Gson();
            RequestBody requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), "{}");

            Request request = new Request.Builder()
                    .url(httpServer + "/transaction/start")
                    .post(requestBody)
                    .build();


            okhttp3.Call call = okHttpClient.newCall(request);
            try (okhttp3.Response response = call.execute()) {
                ResponseBody body = response.body();
                Objects.requireNonNull(body);
                String text = body.string();
                Type type = new TypeToken<Map<String, String>>() {
                }.getType();


                Map<Object, Object> map = gson.fromJson(text, type);
                transactionId = (String) map.get("data");
                Objects.requireNonNull(transactionId);
            }
        }
    }

    public String format(String text) {
        return text;
    }

    public String format(String text, Map<String, Object> queryParameters) {
        Set<String> keys = queryParameters.keySet();
        for (String key : keys) {
            String param = "$" + key;
            if (text.contains(param)) {
                Object value = queryParameters.get(key);
                text = text.replace(param, value.toString());
            }
        }
        return text;
    }

    public UltipaResultSet run(String uql) {
        return run(uql, ImmutableMap.of());
    }

    public UltipaResultSet run(String uql, Map<String, Object> queryParameters) {
        try {
            Gson gson = new Gson();
            String text = format(uql, queryParameters);

            Request request;
            if (transactionId != null) {
                String json = gson.toJson(ImmutableMap.of("transactionId", transactionId, "uql", text));
                RequestBody requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);

                request = new Request.Builder()
                        .url(this.httpServer + "/transaction/run")
                        .post(requestBody)
                        .build();
            } else {
                String json = gson.toJson(ImmutableMap.of("uql", text));
                RequestBody requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);

                request = new Request.Builder()
                        .url(this.httpServer + "/connection/run")
                        .post(requestBody)
                        .build();
            }


            try (okhttp3.Response response = okHttpClient.newCall(request).execute()) {
                ResponseBody body = response.body();
                String body_string = body.string();
                com.ultipa.sdk.operate.response.Response ultipaResponse = gson.fromJson(body_string, com.ultipa.sdk.operate.response.Response.class);
                return new UltipaResultSet(ultipaResponse);
            }
        } catch (IOException exception) {
            catch_exception(exception);
        }
        com.ultipa.sdk.operate.response.Response response = new com.ultipa.sdk.operate.response.Response();
        return new UltipaResultSet(response);
    }

    public static void catch_exception(final Exception exception) {
        exception.printStackTrace();
    }

    public void commit() {
        try {
            if (transactionId != null) {
                Objects.requireNonNull(transactionId);
                Request request = new Request.Builder()
                        .url(httpServer + "/transaction/commit?transactionId=" + transactionId)
                        .get()
                        .build();
                try (okhttp3.Response response = okHttpClient.newCall(request).execute()) {

                }
            }

        } catch (IOException exception) {
            catch_exception(exception);
        }
    }

    public void rollback() {
        try {
            if (transactionId != null) {
                Request request = new Request.Builder()
                        .url(httpServer + "/transaction/rollback?transactionId=" + transactionId)
                        .get()
                        .build();
                try (okhttp3.Response response = okHttpClient.newCall(request).execute()) {

                }
            }
        } catch (IOException exception) {
            catch_exception(exception);
        }
    }

    public void close() {
        try {
            Request request = new Request.Builder()
                    .url(httpServer + "/transaction/close?transactionId=" + transactionId)
                    .get()
                    .build();
            try (okhttp3.Response response = okHttpClient.newCall(request).execute()) {

            }
            transactionId = null;
        } catch (IOException exception) {
            catch_exception(exception);
        }
    }
}
