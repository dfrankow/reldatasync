package org.maventy.reldatasync;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class NetworkUtil {
    private static final String REQUEST_REFERER = "reldatasync";
    private static final int CONNECTION_TIMEOUT_MILISECONDS = 3000;

//    // @see https://mkyong.com/java/java-11-httpclient-examples/
//    public static HttpRequest.BodyPublisher ofFormData(Map<Object, Object> data) {
//        var builder = new StringBuilder();
//        for (Map.Entry<Object, Object> entry : data.entrySet()) {
//            if (builder.length() > 0) {
//                builder.append("&");
//            }
//            builder.append(URLEncoder.encode(
//                    entry.getKey().toString(), StandardCharsets.UTF_8));
//            builder.append("=");
//            builder.append(URLEncoder.encode(
//                    entry.getValue().toString(), StandardCharsets.UTF_8));
//        }
//        return HttpRequest.BodyPublishers.ofString(builder.toString());
//    }

    /**
     * Make an HTTP POST request.
     *
     * @param con
     * @param urlString
     * @param urlParams
     * @throws IOException
     */
    private static HttpURLConnection getResponseFromUrl(
            HttpURLConnection con,
            String urlString,
            String urlParams) throws IOException {
        // Get CSRF Token
        URL url = new URL(urlString);

        // Post the request and get the response
        con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Referer", REQUEST_REFERER);
        con.setConnectTimeout(CONNECTION_TIMEOUT_MILISECONDS);

        if (urlParams != null) {
            con.setRequestProperty("Content-Length",
                    Integer.toString(urlParams.getBytes().length));
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(urlParams);
            wr.flush();
            wr.close();
        }

        // What to do about non-200?
        // if (con.getResponseCode() != 200) {
        // }
        return con;
    }

    /**
     * Make an HTTP request, and return the output as a String.
     *
     * The first request gets the CSRF token from Django, the second POSTs and grabs the output.
     */
    private static String getResponseTextFromUrl(
            String urlString, String urlParams) throws IOException {
        HttpURLConnection con = null;
        try {
            con = getResponseFromUrl(con, urlString, urlParams);
            return fromHttpUrlConnectionToText(con);
        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
    }

    /**
     * Return conn.getContent as a String.
     *
     * @throws java.io.FileNotFoundException if the connection has a 400 error
     */
    private static String fromHttpUrlConnectionToText(
            HttpURLConnection conn) throws IOException {
        StringBuilder text = new StringBuilder();

        int code = conn.getResponseCode();
        if (code != 200) {
//            Log.e(LOGGER_TAG, "Unexpected HTTP response in fromHttpUrlConnectionToText(): "
//                    + code + " - " + conn.getResponseMessage());
        }
        InputStreamReader in = new InputStreamReader((InputStream) conn.getContent());
        BufferedReader buff = new BufferedReader(in);

        String line;
        do {
            line = buff.readLine();
            text.append(line);
        } while (line != null);
        return text.toString();
    }

}
