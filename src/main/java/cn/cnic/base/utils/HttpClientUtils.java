package cn.cnic.base.utils;

import cn.cnic.base.vo.MessageConfig;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Http tool class
 */
public class HttpClientUtils {

	/**
     * Introducing logs, note that they are all packaged under "org.slf4j"
     */
    private static Logger logger = LoggerUtil.getLogger();

	public static String INTERFACE_CALL_ERROR = "Interface call error";

    public static Map<String, String> TOKEN_MAP = new HashMap<>();

    private static String username = "admin";
    private static String password = "admin";

    public static String doGetComCustomizeHeader(String url, Map<String, String> map, Integer timeOutMS, Map<String, String> headerParam) throws Exception {
        if (StringUtils.isBlank(url)) {
            throw new Exception("url is null");
        }
        // Create an "httpclient" object
        CloseableHttpClient httpClient = HttpClients.createDefault();

        // Create a "get" mode request object
        HttpGet httpGet = null;
        // Determine whether to add parameters
        if (null != map && !map.isEmpty()) {
            // Since the parameters of the "GET" request are all assembled behind the "URL" address, we have to build a "URL" with parameters.

            URIBuilder uriBuilder = new URIBuilder(url);

            List<NameValuePair> list = new LinkedList<>();
            for (String key : map.keySet()) {
                if (null == key) {
                    continue;
                }
                list.add(new BasicNameValuePair(key,map.get(key)));
            }

            uriBuilder.setParameters(list);
            // Construct a "GET" request object from a "URI" object with parameters
            httpGet = new HttpGet(uriBuilder.build());
        } else {
            httpGet = new HttpGet(url);
        }

        // Type of transmission
        httpGet.addHeader("Content-type", "application/json");

        // Add request header information
        // Browser representation
        // httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.6)");
        // Type of transmission
        // httpGet.addHeader("Content-Type", "application/x-www-form-urlencoded");
        //add header param
        if (null != headerParam && headerParam.keySet().size() > 0) {
            for (String key : headerParam.keySet()) {
                if (null == key) {
                    continue;
                }
                httpGet.addHeader(key, headerParam.get(key));
            }
        }
        if (null != timeOutMS) {
            // Set timeout
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(5000).setConnectionRequestTimeout(1000)
                    .setSocketTimeout(timeOutMS).build();
            httpGet.setConfig(requestConfig);
        }

        logger.info("call '" + url + "' start");
        // Get the response object by requesting the object
        CloseableHttpResponse response = httpClient.execute(httpGet);
        logger.info("call succeeded,return msg:" + response.toString());
        String result = EntityUtils.toString(response.getEntity(), "utf-8");
        // Get result entity
        // Determine whether the network connection status code is normal (0--200 are normal)
        int statusCode = response.getStatusLine().getStatusCode();
        if (HttpStatus.SC_OK != statusCode && HttpStatus.SC_CREATED !=statusCode) {
            logger.warn("call failed,return msg:" + result);
            throw new Exception(result);
        }
        // Release link
        response.close();
        return result;
    }

    public static String doPostStrComCustomizeHeader(String url, String json, Integer timeOutMS, Map<String, String> headerParam) throws Exception {
        if (StringUtils.isBlank(url)) {
            throw new Exception("url is null");
        }
        // Create a "post" mode request object
        HttpPost httpPost = new HttpPost(url);
        // Set parameters to the request object
        if (StringUtils.isNotBlank(json)) {
            logger.debug("afferent json param:" + json);
            StringEntity stringEntity = new StringEntity(json, ContentType.APPLICATION_JSON);
            stringEntity.setContentEncoding("utf-8");
            httpPost.setEntity(stringEntity);
        }
        httpPost.setProtocolVersion(HttpVersion.HTTP_1_1);
        if (null != timeOutMS) {
            // Set timeout
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(5000).setConnectionRequestTimeout(1000)
                    .setSocketTimeout(timeOutMS).build();
            httpPost.setConfig(requestConfig);
        }

        //add header param
        if (null != headerParam && headerParam.keySet().size() > 0) {
            for (String key : headerParam.keySet()) {
                if (null == key) {
                    continue;
                }
                httpPost.addHeader(key, headerParam.get(key));
            }
        }

        logger.info("call '" + url + "' start");
        String result = doPostComCustomizeHttpPost(httpPost);
        // Close the connection and release the resource
        httpPost.releaseConnection();
        return result;
    }

    public static String doPostComCustomizeHeader(String url, Map<String, String> json, Integer timeOutMS, Map<String, String> headerParam) throws Exception {
        if (StringUtils.isBlank(url)) {
            throw new Exception("url is null");
        }
        // Create a "post" mode request object
        HttpPost httpPost = new HttpPost(url);
        // Set parameters to the request object
        if (null != json) {
            String jsonStr = JsonUtils.toJsonNoException(json);
            return doPostStrComCustomizeHeader(url, jsonStr, timeOutMS, headerParam);
        }
        return doPostStrComCustomizeHeader(url, null, timeOutMS, headerParam);
    }

    public static String doPostComCustomizeHttpPost(HttpPost httpPost) throws Exception {
        if (null == httpPost) {
            return (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":HttpPost is Null");
        }
        // Create an "httpclient" object
        CloseableHttpClient httpClient = null;

        // Create an "httpclient" object
        httpClient = HttpClients.createDefault();
        // Perform the request operation and get the result (synchronous blocking)
        CloseableHttpResponse response = httpClient.execute(httpPost);
        logger.info("call succeeded,return msg:" + response.toString());
        // Get result entity
        // Determine whether the network connection status code is normal (0--200 are normal)
        String result = EntityUtils.toString(response.getEntity(), "utf-8");
        int statusCode = response.getStatusLine().getStatusCode();
        if (HttpStatus.SC_GATEWAY_TIMEOUT == statusCode || (HttpStatus.SC_NOT_FOUND == statusCode && "Unknown resource!".equals(result))) {
            int retryCount = 1;
            while (retryCount <= 3) {
                Thread.sleep(2000);
                response = httpClient.execute(httpPost);
                logger.info("retry count " + retryCount + ", return msg:" + response.toString());
                // Get result entity
                // Determine whether the network connection status code is normal (0--200 are normal)
                result = EntityUtils.toString(response.getEntity(), "utf-8");
                statusCode = response.getStatusLine().getStatusCode();
                if (HttpStatus.SC_NOT_FOUND != statusCode && !"Unknown resource!".equals(result)) {
                    break;
                }
                retryCount++;
            }
        }
        if (HttpStatus.SC_OK != statusCode && HttpStatus.SC_CREATED !=statusCode) {
            logger.warn("call failed,return msg:" + result);
            throw new Exception(result);
        }
        logger.info("call succeeded,return msg:" + result);
        return result;
    }

    public static String doPostFromComCustomizeHeader(String url, Map<String, Object> json, Integer timeOutMS, Map<String, String> headerParam) throws Exception {
        if (StringUtils.isBlank(url)) {
            throw new Exception("url is null");
        }
        // Create a "post" mode request object
        HttpPost httpPost = new HttpPost(url);
        // Set parameters to the request object
        if (null != json && json.keySet().size() > 0) {
            //Create parameter queue
            List<NameValuePair> formParams = new ArrayList<>();
            for (String key : json.keySet()) {
                if (null == key || null == json.get(key)) {
                    continue;
                }
                formParams.add(new BasicNameValuePair(key, json.get(key).toString()));
            }
            UrlEncodedFormEntity uefEntity;
            uefEntity = new UrlEncodedFormEntity(formParams, "UTF-8");
            httpPost.setEntity(uefEntity);
        }
        httpPost.setProtocolVersion(HttpVersion.HTTP_1_1);
        if (null != timeOutMS) {
            // Set timeout
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(5000).setConnectionRequestTimeout(1000)
                    .setSocketTimeout(timeOutMS).build();
            httpPost.setConfig(requestConfig);
        }
        //add header param
        if (null != headerParam && headerParam.keySet().size() > 0) {
            for (String key : headerParam.keySet()) {
                if (null == key) {
                    continue;
                }
                httpPost.addHeader(key, headerParam.get(key));
            }
        }
        logger.info("call '" + url + "' start");
        String result = doPostComCustomizeHttpPost(httpPost);
        // Close the connection and release the resource
        httpPost.releaseConnection();
        return result;
    }

    public static CloseableHttpResponse doPostCloseableHttpResponse(String url, Map<String, String> paramMap, Integer timeOutMS, Map<String, String> headerParam) throws IOException {
        CloseableHttpClient client = null;
        if (null != timeOutMS) {
            RequestConfig requestConfig = creatRequestConfig(timeOutMS);
            client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        } else {
            client = HttpClientBuilder.create().build();
        }
        //return is inputStream


        HttpPost post = new HttpPost(url);
        //add header param
        if (null != headerParam && headerParam.keySet().size() > 0) {
            for (String key : headerParam.keySet()) {
                if (null == key) {
                    continue;
                }
                post.addHeader(key, headerParam.get(key));
            }
        }
        // Set parameters to the request object
        if (null != paramMap && paramMap.keySet().size() > 0) {
            //Create parameter queue
            List<NameValuePair> formParams = new ArrayList<>();
            for (String key : paramMap.keySet()) {
                if (null == key) {
                    continue;
                }
                formParams.add(new BasicNameValuePair(key, paramMap.get(key)));
            }
            UrlEncodedFormEntity uefEntity;
            uefEntity = new UrlEncodedFormEntity(formParams, "UTF-8");
            post.setEntity(uefEntity);
        }
        logger.info("call '" + url + "' start");
        return client.execute(post);
    }

    public static CloseableHttpResponse doPostCloseableHttpResponseParamStr(String url, String param, Integer timeOutMS, Map<String, String> headerParam) throws IOException {
        CloseableHttpClient client = null;
        if (null != timeOutMS) {
            RequestConfig requestConfig = creatRequestConfig(timeOutMS);
            client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        } else {
            client = HttpClientBuilder.create().build();
        }
        HttpPost post = new HttpPost(url);
        //add header param
        if (null != headerParam && headerParam.keySet().size() > 0) {
            for (String key : headerParam.keySet()) {
                if (null == key) {
                    continue;
                }
                post.addHeader(key, headerParam.get(key));
            }
        }
        // Set parameters to the request object
        if (StringUtils.isNotBlank(param)) {
            post.setEntity(new StringEntity(param,ContentType.APPLICATION_JSON));
        }
        logger.info("call '" + url + "' start");
        return client.execute(post);
    }

    public static CloseableHttpResponse doGetCloseableHttpResponse(String url, Map<String, String> paramMap, Integer timeOutMS, Map<String, String> headerParam) throws IOException, URISyntaxException {
        if (StringUtils.isBlank(url)) {
        }
        CloseableHttpClient client;
        if (null != timeOutMS) {
            RequestConfig requestConfig = creatRequestConfig(timeOutMS);
            client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        } else {
            client = HttpClientBuilder.create().build();
        }
        //return is inputStream

        // Create a "get" mode request object
        HttpGet httpGet;
        // Determine whether to add parameters
        if (null != paramMap && !paramMap.isEmpty()) {
            // Since the parameters of the "GET" request are all assembled behind the "URL" address, we have to build a "URL" with parameters.

            URIBuilder uriBuilder = new URIBuilder(url);

            List<NameValuePair> list = new LinkedList<>();
            for (String key : paramMap.keySet()) {
                BasicNameValuePair param = new BasicNameValuePair(key, paramMap.get(key));
                list.add(param);
            }

            uriBuilder.setParameters(list);
            // Construct a "GET" request object from a "URI" object with parameters
            httpGet = new HttpGet(uriBuilder.build());
        } else {
            httpGet = new HttpGet(url);
        }

        // Add request header information
        if (null != headerParam && headerParam.keySet().size() > 0) {
            for (String key : headerParam.keySet()) {
                if (null == key) {
                    continue;
                }
                httpGet.addHeader(key, headerParam.get(key));
            }
        }

        logger.info("call '" + url + "' start");
        // Get the response object by requesting the object
        // Get result entity
        // Determine whether the network connection status code is normal (0--200 are normal)
        // Release link
        return client.execute(httpGet);
    }

    public static Map<String, String> setHeader(Map<String, String> headerParam) {
        if (null == headerParam) {
            headerParam = new HashMap<>();
        }
        headerParam.put("Accept","*/*");
        headerParam.put("Accept-Encoding","gzip, deflate");
        headerParam.put("Accept-Language","zh-CN,zh;q=0.9");
        headerParam.put("Connection","keep-alive");
        //headerParam.put("Content-Length","29");
        headerParam.put("Content-Type","application/x-www-form-urlencoded;charset=UTF-8");
        //headerParam.put("Host","10.0.90.221:6421");
        headerParam.put("Origin","chrome-extension://ieoejemkppmjcdfbnfphhpbfmallhfnc");
        headerParam.put("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36");
        //headerParam.put("Sec-Fetch-Mode","cors");
        //headerParam.put("Origin","https://wcm.cdstm.cn");
        //headerParam.put("X-Requested-With","XMLHttpRequest");
        //headerParam.put("Sec-Fetch-Site","same-origin");
        return headerParam;
    }

    public static Map<String, String> setHeaderContentType(String contentType) {
        if (StringUtils.isBlank(contentType)) {
            return null;
        }
        Map<String, String> headerParam = new HashMap<>();
        headerParam.put("Content-type", "application/json");
        return headerParam;
    }

    public static Map<String, String> setToken(String url, Map<String, String> headerParam) throws Exception {
        if (null == headerParam) {
            headerParam = new HashMap<>();
        }
        if (StringUtils.isBlank(url)) {
            throw new Exception("url is null");
        }
        String token = TOKEN_MAP.get(url);
        logger.info("token01=" + token);
        if (StringUtils.isNotBlank(token)) {
            headerParam.put("Authorization", "Bearer " + token);
            return headerParam;
        }
        Integer timeOutMS = 10000;
        Map<String, Object> json = new HashMap<>();
        json.put("username", username);
        json.put("password", password);
        String doPostComCustomizeHeader = doPostFromComCustomizeHeader(url + "/piflow-web/jwtLogin", json, timeOutMS, setHeader(null));
        logger.info("Return information：" + doPostComCustomizeHeader);
        if (StringUtils.isBlank(doPostComCustomizeHeader)) {
            throw new Exception("Error : " + MessageConfig.INTERFACE_RETURN_VALUE_IS_NULL_MSG());
        }
        // Convert a json string to a json object
        JSONObject obj = JSONObject.fromObject(doPostComCustomizeHeader);
        String code = obj.getString("code");
        if (!"200".equals(code)) {
            throw new Exception(doPostComCustomizeHeader);
        }
        token = obj.getString("token");
        logger.info("token02=" + token);
        TOKEN_MAP.put(url, token);
        headerParam.put("Authorization", "Bearer " + token);
        return headerParam;
    }

    private static RequestConfig creatRequestConfig(int timeout) {
        return RequestConfig.custom()
                .setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000)
                .setSocketTimeout(timeout * 1000).build();
    }
    public static Map<String, String> setTokenWithCurrReq(Map<String, String> headerParam) throws Exception {
        if (null == headerParam) {
            headerParam = new HashMap<>();
        }
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        String token = request.getHeader("Authorization");
        if (StringUtils.isNotBlank(token)) {
            headerParam.put("Authorization", token);
        }
        return headerParam;
    }
}