package com.github.apengda.fifio.elasticsearch.util;

import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsUtil {

    public static RestHighLevelClient createClient(final DbInfo dbInfo) {
        return createClient(dbInfo.getUrl(), dbInfo.getUsername(), dbInfo.getPassword());
    }

    public static RestHighLevelClient createClient(String urlWithHostAndPort, String username, String password) {
        final List<HttpHost> httpHostList = new ArrayList<>();
        final String[] hosts = urlWithHostAndPort.split(",");
        for (String h : hosts) {
            String[] sp = h.split(":");
            if (sp.length != 3) {
                throw new IllegalArgumentException("illegal elasticsearch url:" + h);
            }
            httpHostList.add(new HttpHost(sp[1].replace("//", "").trim(), Integer.parseInt(sp[2]), sp[0]));
        }

        final RestClientBuilder builder = RestClient.builder(httpHostList.toArray(new HttpHost[0]));

        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            final CredentialsProvider credentialsProvider =
                    new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(
                        HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        }
        return new RestHighLevelClient(builder);
    }

    public static Integer shards(RestHighLevelClient client, String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        getIndexRequest.features(GetIndexRequest.Feature.SETTINGS);

        GetIndexResponse resp = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        Settings settings = resp.getSettings().values().iterator().next();

        return settings.getAsInt("index.number_of_shards", 1);
    }

    public static String get(RestClient client, String url, Map<String, String> params, HttpEntity entity) throws IOException {
        return request(client, "GET", url, params, entity);
    }

    public static String post(RestClient client, String url, Map<String, String> params, HttpEntity entity) throws IOException {
        return request(client, "GET", url, params, entity);
    }

    public static String put(RestClient client, String url, HttpEntity entity) throws IOException {
        return request(client, "PUT", url, null, entity);
    }

    private static String request(RestClient client, String method, String url, Map<String, String> params, HttpEntity entity) throws IOException {
        if (!url.contains("?")) {
            url += "?format=json";
        } else {
            url += "&format=json";
        }
        Request request = new Request(method, url);
        if (MapUtils.isNotEmpty(params)) {
            params.entrySet().forEach(t -> request.addParameter(t.getKey(), t.getValue()));
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        Response response = client.performRequest(request);
        if (response != null && response.getStatusLine().getStatusCode() == 200) {
            return EntityUtils.toString(response.getEntity());
        } else {
            throw new IOException("elasticsearch request failed!" + response.getStatusLine().getStatusCode());
        }
    }

}
