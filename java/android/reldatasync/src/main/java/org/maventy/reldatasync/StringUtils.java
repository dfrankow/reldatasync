package org.maventy.reldatasync;

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
}
