package cn.cnic.protocol.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class UrlElement {
    // 带后缀的文件名
    String fileName;
    // 单位bytes
    long size;
    // url
    String httpUrl;
}