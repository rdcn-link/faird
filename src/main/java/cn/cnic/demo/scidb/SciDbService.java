package cn.cnic.demo.scidb;

import cn.cnic.demo.scidb.domain.*;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.model.MetaData;
import cn.cnic.protocol.parser.FileUrlParser;
import cn.cnic.protocol.service.FairdService;
import cn.cnic.protocol.vo.UrlElement;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HtmlUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.ivy.core.module.descriptor.License;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.google.common.collect.Lists;
import org.springframework.util.CollectionUtils;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: 张文斌
 * @Data: 2023/10/31 15
 * @Description:
 */
public class SciDbService implements FairdService {

    @Override
    public MetaData getMeta(String identifier) {

        JSONArray array = JSONUtil.createArray();
        JSONObject o = JSONUtil.createObj();
        JSONObject obj = JSONUtil.createObj();
        obj.putOnce("bilingual","false").putOnce("field","doi").putOnce("field_value",identifier).putOnce("relation","and");
        array.add(obj);
        o.putOnce("parameters",array);

        String result = HttpUtil.post("https://www.scidb.cn/api/sdb-query-service/public/query.advance",o.toString());

        if(null == result || !JSONUtil.isJsonObj(result)){
            return null;
        }
        JSONObject parse = JSONUtil.parseObj(result);
        String code = parse.getStr("code");
        if(!StrUtil.equals("20000",code)){
            return null;
        }
        JSONObject datas = parse.getJSONObject("data");
        if(null == datas){
            return null;
        }

        JSONArray o1 = datas.getJSONArray("data");
        if(CollectionUtils.isEmpty(o1) || o1.size() == 0){
            return null;
        }
        JSONObject o2 = (JSONObject)o1.get(0);
        String doi = o2.getStr("doi");
        if(!StrUtil.equals(doi,identifier)){
            return null;
        }
        DataSet one = JSONUtil.toBean(o2, DataSet.class);

        MetaData metaData = new MetaData();
        MetaData.Title titleZh = new MetaData.Title();
        titleZh.setTitle(one.getTitleZh());
        titleZh.setLanguage("zh");
        MetaData.Title titleEn = new MetaData.Title();
        titleEn.setTitle(one.getTitleEn());
        titleEn.setLanguage("en");
        List<MetaData.Title> titles = Lists.newArrayList();
        titles.add(titleZh);
        titles.add(titleEn);
        metaData.setTitle(titles);


        MetaData.Identifier id = new MetaData.Identifier();
        id.setId(one.getDoi());
        id.setType("DOI");
        List<MetaData.Identifier> identifiers = Lists.newArrayList();
        identifiers.add(id);
        metaData.setIdentifier(identifiers);


        metaData.setUrl("https://www.doi.org/"+one.getDoi());
        metaData.setDescription(HtmlUtil.cleanHtmlTag(one.getIntroductionZh()));
        metaData.setKeywords(String.join(",",one.getKeywordZh()));

        MetaData.Date createDate = new MetaData.Date();
        createDate.setDateTime(DateUtil.format(one.getVersionCreateDate(),"yyyy/MM/dd"));
        createDate.setType("Created");
        MetaData.Date issuedDate = new MetaData.Date();
        issuedDate.setDateTime(DateUtil.format(one.getVersionPublishDate(),"yyyy/MM/dd"));
        issuedDate.setType("Issued");
        MetaData.Date updatedDate = new MetaData.Date();
        updatedDate.setDateTime(DateUtil.format(one.getVersionUpdateDate(),"yyyy/MM/dd"));
        updatedDate.setType("Updated");

        List<MetaData.Date> dates = Lists.newArrayList();
        dates.add(createDate);
        dates.add(issuedDate);
        dates.add(updatedDate);
        metaData.setDates(dates);

        List<Taxonomy> taxonomy = one.getTaxonomy();
        List<String> subjects = taxonomy.stream()
                .map(Taxonomy::getNameZh).collect(Collectors.toList());
        metaData.setSubject(String.join(";", subjects));

        List<MetaData.Creator> creators = Lists.newArrayList();
        List<Author> author = one.getAuthor();
        if(!CollectionUtils.isEmpty(author)){
            for (int i = 0; i < author.size(); i++) {
                MetaData.Creator creator = new MetaData.Creator();
                creator.setCreatorName(author.get(i).getNameZh());

                List<Organization> organizations = author.get(i).getOrganizations();
                if(!CollectionUtils.isEmpty(organizations)){
                    creator.setAffiliation(organizations.get(0).getNameZh());
                }

                creators.add(creator);
            }
        }
        metaData.setCreators(creators);

        MetaData.Publisher publisher = new MetaData.Publisher();
        publisher.setName("ScienceDB");
        publisher.setUrl("http://www.scidb.cn");
        metaData.setPublisher(publisher);

        metaData.setEmail(one.getCorrespondent());

        String resultf = HttpUtil.get("https://www.scidb.cn/api/gin-sdb-filetree/public/file/getTreeList?datasetId="+one.getDataSetId()+"&version="+one.getVersion());
        if(null == resultf || !JSONUtil.isJsonObj(resultf)){
            return null;
        }
        JSONObject rparse = JSONUtil.parseObj(resultf);
        String rcode = rparse.getStr("code");
        if(!StrUtil.equals("20000",rcode)){
            return null;
        }
        JSONObject rdatas = rparse.getJSONObject("data");
        if(null == rdatas){
            return null;
        }
        TreeItem bean = JSONUtil.toBean(rdatas, TreeItem.class);
        Map<String, Object> map = Maps.newHashMap();
        List<UrlElement> r = Lists.newArrayList();
        Set<String> sets = Sets.newHashSet();
        Map<String, Object> dataMap = getDataFileList(map, sets, r, bean);
        Set<String> s = (Set<String>)dataMap.get("s");
        if(!CollectionUtils.isEmpty(s)){
            metaData.setFormat(String.join(",",s));
        }else {
            metaData.setFormat("");
        }


        List<MetaData.Size> sizes = Lists.newArrayList();
        MetaData.Size size = new MetaData.Size();
        size.setUnitText("bytes");
        size.setValue(String.valueOf(one.getSize()));
        sizes.add(size);
        metaData.setSize(sizes);

        License copyRight = one.getCopyRight();
        List<String> license = Lists.newArrayList();
        if(null != copyRight){
            license.add(copyRight.getUrl());
        }
        metaData.setLicense(license);

        metaData.setLanguage(one.getLanguage());
        metaData.setVersion(one.getVersion());
        return metaData;
    }

    @Override
    public List<DataFrame> getData(String identifier) {
        List<DataFrame> dataFile = getDataFile(identifier);
        return dataFile;
    }

    private List<DataFrame> getDataFile(String identifier){

        JSONArray array = JSONUtil.createArray();
        JSONObject o = JSONUtil.createObj();
        JSONObject obj = JSONUtil.createObj();
        obj.putOnce("bilingual","false").putOnce("field","doi").putOnce("field_value",identifier).putOnce("relation","and");
        array.add(obj);
        o.putOnce("parameters",array);

        String result = HttpUtil.post("https://www.scidb.cn/api/sdb-query-service/public/query.advance",o.toString());

        if(null == result || !JSONUtil.isJsonObj(result)){
            return null;
        }
        JSONObject parse = JSONUtil.parseObj(result);
        String code = parse.getStr("code");
        if(!StrUtil.equals("20000",code)){
            return null;
        }
        JSONObject datas = parse.getJSONObject("data");
        if(null == datas){
            return null;
        }

        JSONArray o1 = datas.getJSONArray("data");
        if(CollectionUtils.isEmpty(o1) || o1.size() == 0){
            return null;
        }
        JSONObject o2 = (JSONObject)o1.get(0);
        String doi = o2.getStr("doi");
        if(!StrUtil.equals(doi,identifier)){
            return null;
        }
        DataSet one = JSONUtil.toBean(o2, DataSet.class);

        String resultf = HttpUtil.get("https://www.scidb.cn/api/gin-sdb-filetree/public/file/getTreeList?datasetId="+one.getDataSetId()+"&version="+one.getVersion());
        if(null == resultf || !JSONUtil.isJsonObj(resultf)){
            return null;
        }
        JSONObject parsef = JSONUtil.parseObj(resultf);
        String codef = parsef.getStr("code");
        if(!StrUtil.equals("20000",codef)){
            return null;
        }
        JSONObject datasf = parsef.getJSONObject("data");
        if(null == datasf){
            return null;
        }
        TreeItem bean = JSONUtil.toBean(datasf, TreeItem.class);
        Map<String, Object> map = Maps.newHashMap();
        List<UrlElement> r = Lists.newArrayList();
        Set<String> sets = Sets.newHashSet();
        Map<String, Object> dataFileList = getDataFileList(map, sets, r, bean);
        List<UrlElement> ts = (List<UrlElement>)dataFileList.get("r");

        FileUrlParser parser = new FileUrlParser();
        Dataset<Row> dataFrame = parser.toSparkDataFrame(ts);
        DataFrame dataX = new DataFrame(dataFrame);
        List<DataFrame> dataXList = new ArrayList<>();
        dataXList.add(dataX);

        return dataXList;
    }

    private Map<String, Object> getDataFileList(Map<String, Object> map,Set<String> s,List<UrlElement> r,TreeItem t){
        boolean dir = t.isDir();
        if(dir){
            List<TreeItem> children = t.getChildren();
            if(!CollectionUtils.isEmpty(children)){
                for (TreeItem child : children) {
                    getDataFileList(map,s,r,child);
                }
            }
        }else {
            String type = t.getType();
            if(StrUtil.equals(type,"file")){
                String label = t.getLabel();
                UrlElement urlElement0 = new UrlElement(label, t.getSize(), "https://download.scidb.cn/download?fileId="+t.getId());
                r.add(urlElement0);

                String[] split = label.split("\\.");
                s.add(split[split.length-1]);
            }
        }
        map.put("r",r);
        map.put("s",s);
        return map;
    }
}
