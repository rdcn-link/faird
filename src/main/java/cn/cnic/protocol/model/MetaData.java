package cn.cnic.protocol.model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * @author yaxuan
 * @create 2023/8/29 11:11
 */
@Getter
@Setter
@Data
public class MetaData implements Serializable {

    private List<Title> title;
    private List<Identifier> identifier;
    private String url;
    private String description;
    private String keywords;
    private List<Date> dates;
    private String subject;
    private List<Creator> creators;
    private Publisher publisher;
    private List<String> email;
    private String format;
    private List<Size> size;
    private List<String> license;
    private String language;
    private String version;

    @Data
    public static class Title {
        private String title;
        private String language;
    }

    @Data
    public static class Identifier {
        private String id;
        private String type;
    }

    @Data
    public static class Date {
        private String dateTime;
        private String type;
    }

    @Data
    public static class Creator {
        private String creatorName;
        private String affiliation;
    }

    @Data
    public static class Publisher {
       private String name;
       private String url;
    }

    @Data
    public static class Size {
        private String value;
        private String unitText;
    }
}
