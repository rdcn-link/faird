package cn.cnic.base.utils;

import cn.cnic.base.vo.MessageConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;

/**
 * Http tool class
 */
public class HttpUtils {

	/**
     * Introducing logs, note that they are all packaged under "org.slf4j"
     */
    private static Logger logger = LoggerUtil.getLogger();
	
    /**
     * "post" request to transfer "json" data
     *
     * @param url
     * @param json
     * @param timeOutMS (Millisecond)
     * @return
     */
    public static String doPostParmaMap(String url, Map<?, ?> json, Integer timeOutMS) {
    	if (null == json) {
    		return doPost(url, null, timeOutMS);
    	}
        String formatJson = JsonUtils.toFormatJsonNoException(json);
        return doPost(url, formatJson, timeOutMS);
    }

    /**
     * "post" request to transfer "json" data
     *
     * @param url
     * @param json
     * @param timeOutMS (Millisecond)
     * @return
     */
    public static String doPost(String url, String json, Integer timeOutMS) {
        return doPostComCustomizeHeader(url, json, timeOutMS, null);
    }

    /**
     * Get request to transfer data
     *
     * @param url
     * @param map       Request incoming parameters ("key" is the parameter name, "value" is the parameter value) "map" can be empty
     * @param timeOutMS (Millisecond)
     * @return
     */
    public static String doGet(String url, Map<String, String> map, Integer timeOutMS) {
        if (null == map || map.isEmpty()) {
            return doGetComCustomizeHeader(url, null, timeOutMS, null);
        }
        Map<String, String> mapObject = new HashMap<>();
        for (String key : map.keySet()) {
            mapObject.put(key, map.get(key));
        }
        return doGetComCustomizeHeader(url, mapObject, timeOutMS, null);
    }

    public static String doPostComCustomizeHeader(String url, String json, Integer timeOutMS, Map<String, String> herderParam) {
        String result = "";

        // Create a "post" mode request object
        HttpPost httpPost = null;
        try {
            // Create a "post" mode request object
            httpPost = new HttpPost(url);
            logger.debug("afferent json param:" + json);
            // Set parameters to the request object
            if (StringUtils.isNotBlank(json)) {
                StringEntity stringEntity = new StringEntity(json, ContentType.APPLICATION_FORM_URLENCODED);
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
            if (null != herderParam && herderParam.keySet().size() > 0) {
                for (String key : herderParam.keySet()) {
                    if (null == key) {
                        continue;
                    }
                    httpPost.addHeader(key, herderParam.get(key));
                }
            }

            logger.info("call '" + url + "' start");
            result = doPostComCustomizeHttpPost(httpPost);
        } catch (UnsupportedCharsetException e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":UnsupportedCharsetException");
        } catch (ParseException e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":ParseException");
        } finally {
            // Close the connection and release the resource
            httpPost.releaseConnection();
        }
        return result;
    }

    public static String doPostMapComCustomizeHeader(String url, Map<?, ?> json, Integer timeOutMS, Map<String, String> herderParam) {
        if (null == json) {
            return doPostComCustomizeHeader(url, null, timeOutMS, herderParam);
        }
        String formatJson = JsonUtils.toFormatJsonNoException(json);
        return doPostComCustomizeHeader(url, formatJson, timeOutMS, herderParam);
    }

    public static String doPostComCustomizeHttpPost(HttpPost httpPost) {
        if (null == httpPost) {
            return (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":HttpPost is Null");
        }
        String result = "";
        // Create an "httpclient" object
        CloseableHttpClient httpClient = null;
        try {
            // Create an "httpclient" object
            httpClient = HttpClients.createDefault();
            // Perform the request operation and get the result (synchronous blocking)
            CloseableHttpResponse response = httpClient.execute(httpPost);
            logger.info("call succeeded,return msg:" + response.toString());
            // Get result entity
            // Determine whether the network connection status code is normal (0--200 are normal)
            switch (response.getStatusLine().getStatusCode()) {
                case HttpStatus.SC_OK:
                    result = EntityUtils.toString(response.getEntity(), "utf-8");
                    logger.info("call succeeded,return msg:" + result);
                    break;
                case HttpStatus.SC_CREATED:
                    result = EntityUtils.toString(response.getEntity(), "utf-8");
                    logger.info("call succeeded,return msg:" + result);
                    break;
                default:
                    result = MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":" + EntityUtils.toString(response.getEntity(), "utf-8");
                    logger.warn("call failed,return msg:" + result);
                    break;
            }
        } catch (UnsupportedCharsetException e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":UnsupportedCharsetException");
        } catch (ClientProtocolException e) {
        	e.getMessage();
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":ClientProtocolException");
        } catch (ParseException e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":ParseException");
        } catch (IOException e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":IOException");
        } catch (Exception e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":Exception");
        }
        return result;
    }

    public static String doGetComCustomizeHeader(String url, Map<String, String> map, Integer timeOutMS, Map<String, String> herderParam) {
        String result = "";
        if (StringUtils.isNotBlank(url)) {
            try {
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
                // httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 5.1;
                // en-US; rv:1.7.6)");
                // Type of transmission
                // httpGet.addHeader("Content-Type", "application/x-www-form-urlencoded");
                //add header param
                if (null != herderParam && herderParam.keySet().size() > 0) {
                    for (String key : herderParam.keySet()) {
                        if (null == key) {
                            continue;
                        }
                        httpGet.addHeader(key, herderParam.get(key));
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
                // Get result entity
                // Determine whether the network connection status code is normal (0--200 are normal)
                switch (response.getStatusLine().getStatusCode()) {
                    case HttpStatus.SC_OK:
                        result = EntityUtils.toString(response.getEntity(), "utf-8");
                        logger.info("call succeeded,return msg:" + result);
                        break;
                    case HttpStatus.SC_CREATED:
                        result = EntityUtils.toString(response.getEntity(), "utf-8");
                        logger.info("call succeeded,return msg:" + result);
                        break;
                    default:
                        result = MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":" + EntityUtils.toString(response.getEntity(), "utf-8");
                        logger.warn("call failed,return msg:" + result);
                        break;
                }
                // Release link
                response.close();
            } catch (ClientProtocolException e) {
                logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
                result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":ClientProtocolException");
            } catch (ParseException e) {
                logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
                result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":ParseException");
            } catch (IOException e) {
                logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
                result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":IOException");
            } catch (URISyntaxException e) {
                logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
                result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":URISyntaxException");
            }
        }
        return result;
    }

    public static String doPostFromComCustomizeHeader(String url, Map<String, String> json, Integer timeOutMS, Map<String, String> herderParam) {
        String result = "";

        // Create a "post" mode request object
        HttpPost httpPost = null;
        try {
            // Create a "post" mode request object
            httpPost = new HttpPost(url);
            // Set parameters to the request object
            if (null != json && json.keySet().size() > 0) {
                //Create parameter queue
                List<NameValuePair> formParams = new ArrayList<>();
                for (String key : json.keySet()) {
                    if (null == key) {
                        continue;
                    }
                    formParams.add(new BasicNameValuePair(key, json.get(key)));
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
            if (null != herderParam && herderParam.keySet().size() > 0) {
                for (String key : herderParam.keySet()) {
                    if (null == key) {
                        continue;
                    }
                    httpPost.addHeader(key, herderParam.get(key));
                }
            }
            logger.info("call '" + url + "' start");
            result = doPostComCustomizeHttpPost(httpPost);
        } catch (UnsupportedCharsetException e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":UnsupportedCharsetException");
        } catch (UnsupportedEncodingException e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":UnsupportedEncodingException");
        } catch (ParseException e) {
            logger.error(MessageConfig.INTERFACE_CALL_ERROR_MSG(), e);
            result = (MessageConfig.INTERFACE_CALL_ERROR_MSG() + ":ParseException");
        } finally {
            // Close the connection and release the resource
            httpPost.releaseConnection();
        }
        return result;
    }

    public static Map<String, String> setHeader(Map<String, String> herderMap) {
        Map<String, String> herderParam = new HashMap<>();
        herderParam.put("Accept","*/*");
        herderParam.put("Accept-Encoding","gzip, deflate");
        herderParam.put("Accept-Language","zh-CN,zh;q=0.9");
        herderParam.put("Connection","keep-alive");
        //herderParam.put("Content-Length","29");
        herderParam.put("Content-Type","application/x-www-form-urlencoded;charset=UTF-8");
        //herderParam.put("Host","10.0.90.221:6421");
        herderParam.put("Origin","chrome-extension://ieoejemkppmjcdfbnfphhpbfmallhfnc");
        herderParam.put("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36");
        //herderParam.put("Sec-Fetch-Mode","cors");
        //herderParam.put("Origin","https://wcm.cdstm.cn");
        //herderParam.put("X-Requested-With","XMLHttpRequest");
        //herderParam.put("Sec-Fetch-Site","same-origin");
        if (null == herderMap || herderMap.isEmpty()) {
            return herderParam;
        }
        herderParam.putAll(herderMap);
        return herderParam;
    }

}
