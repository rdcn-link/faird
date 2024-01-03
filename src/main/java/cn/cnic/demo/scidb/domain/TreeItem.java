package cn.cnic.demo.scidb.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: 张文斌
 * @Data: 2023/10/31 17
 * @Description:
 */
@Data
@NoArgsConstructor
public class TreeItem implements Serializable {

    String      id;
    String      parentId;
    String      label;
    String      path;
    String      type;
    boolean     dir;
    long       size;
    String      signature;
    String      from;
    String      url;
    long        canPreview;
    List<TreeItem> children;
}
