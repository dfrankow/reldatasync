package org.maventy.reldatasync;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class StringUtils {
    /**
     * Join elements with delimiter.
     *
     * This is a backport for Android API 21.
     *
     * @param delim  Delimiter
     * @param elements  Elements
     * @return  Joined string
     */
    public static String join(CharSequence delim, List<? extends CharSequence> elements) {
        if (elements == null || elements.size() == 0) return "";

        StringBuilder sb = new StringBuilder();
        int last = elements.size()-1;
        for (int idx = 0; idx < elements.size(); idx++) {
            sb.append(elements.get(idx));
            if (idx != last) {
                sb.append(delim);
            }
        }

        return sb.toString();
    }

    /**
     * Throwable stack trace to String.
     *
     * @param th  Throwable
     * @return  String
     */
    public static String stackTraceToString(Throwable th) {
        // see https://stackoverflow.com/a/1149712
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        th.printStackTrace(pw);
        return sw.toString();
    }
}
