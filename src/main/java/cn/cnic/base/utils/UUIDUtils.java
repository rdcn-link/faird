package cn.cnic.base.utils;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UUIDUtils {

    /**
     * uuid(32-bit)
     *
     * @return
     */
    public static String getUUID32() {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase();
    }

    public static String getFairURI(String domainName, String identifier) {
        return "fair://" + domainName + "/" + identifier;
    }

    public static String getIdentifier(String uri) {
        // 匹配id的正则表达式
        Pattern pattern = Pattern.compile("fair://[^/]+/(.+)$");
        Matcher matcher = pattern.matcher(uri);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }
}
