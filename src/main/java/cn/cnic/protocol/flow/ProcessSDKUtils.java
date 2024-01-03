package cn.cnic.protocol.flow;

import cn.cnic.base.utils.JsonUtils;
import cn.cnic.base.utils.UUIDUtils;
import cn.cnic.base.vo.MessageConfig;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeanUtils;

import java.util.*;

public class ProcessSDKUtils {

    public static ProcessSDK flowSDKToProcessSDK(Flow flow, boolean isAddId) throws Exception{
        if (null == flow) {
            return null;
        }
        ProcessSDK processSDK = new ProcessSDK();

        // Copy flow information to process
        BeanUtils.copyProperties(flow, processSDK);
        if (isAddId) {
            processSDK.setId(UUIDUtils.getUUID32());
        } else {
            processSDK.setId(null);
        }
        //Set default
        processSDK.setProcessParentType(ProcessParentType.PROCESS);
        // set flowId
//        processSDK.setFlowId(flow.getId());
        // Stops to remove flow
        List<Stop> stopsList = flow.getStopsList();
        // stopsList isEmpty
        if (null != stopsList && stopsList.size() > 0) {
            // List of stop of process
            List<ProcessStopSDK> processStopSDKList = new ArrayList<>();
            // Loop stopsList
            for (Stop stop : stopsList) {
                ProcessStopSDK processStopSDK = stopsToProcessStop(stop, isAddId);
                if(null == processStopSDK) {
                    continue;
                }
                processStopSDKList.add(processStopSDK);
            }
            processSDK.setProcessStopList(processStopSDKList);
        }
        // Get the paths information of flow
        List<Path> pathsList = flow.getPathsList();
        // isEmpty
        if (null != pathsList && pathsList.size() > 0) {
            List<ProcessPathSDK> processPathSDKList = new ArrayList<>();
            // Loop paths information
            for (Path path : pathsList) {
                // isEmpty
                if (null == path) {
                    continue;
                }
                ProcessPathSDK processPathSDK = new ProcessPathSDK();
                // Copy paths information into processPath
                BeanUtils.copyProperties(path, processPathSDK);
                if (isAddId) {
                    processPathSDK.setId(UUIDUtils.getUUID32());
                } else {
                    processPathSDK.setId(null);
                }
                processPathSDKList.add(processPathSDK);
            }
            processSDK.setProcessPathList(processPathSDKList);
        }
        return processSDK;
    }

    public static ProcessStopSDK stopsToProcessStop(Stop stop, boolean isAddId) throws Exception{
        // isEmpty
        if (null == stop) {
            return null;
        }
        ProcessStopSDK processStopSDK = new ProcessStopSDK();
        // Copy stops information into processStop
        BeanUtils.copyProperties(stop, processStopSDK);
        if (isAddId) {
            processStopSDK.setId(UUIDUtils.getUUID32());
        } else {
            processStopSDK.setId(null);
        }
        //数据源,无论是本地数据源还是常用协同数据源,都在拼flowJson时,只拼一个dataCenter和dataSourceId,不拼property;
        //根据dataCenter分发到自己的节点后,在server端根据dataSourceId去查询属性完善flowJson
        switch (stop.getNodeType()){
            case STOP:
            case CUSTOMIZESTOP:
            case COMMONLYUSEDSTOP: {
                //无论是本地算法还是协同算法,属性值都已经保存到flow_stop_property表中了,已经从数据库查询赋值到stops.getProperties()中
                // Remove the properties of stops
                List<Property> properties = stop.getProperties();
                // Determine if the stops attribute is empty
                if (null != properties && properties.size() > 0) {
//                Map<String, String> dataSourcePropertyMap = DataSourceUtils.dataSourceToPropertyMap(dataSource);
                    List<ProcessStopPropertySDK> processStopPropertySDKList = new ArrayList<>();
                    // Attributes of loop stops
                    for (Property property : properties) {
                        // isEmpty
                        if (null == property) {
                            continue;
                        }
                        ProcessStopPropertySDK processStopPropertySDK = new ProcessStopPropertySDK();
                        // Copy property information into processStopProperty
                        BeanUtils.copyProperties(property, processStopPropertySDK);
                        if (isAddId) {
                            processStopPropertySDK.setId(UUIDUtils.getUUID32());
                        } else {
                            processStopPropertySDK.setId(null);
                        }
                        // Associated foreign key
                        processStopPropertySDKList.add(processStopPropertySDK);
                    }
                    processStopSDK.setProcessStopPropertyList(processStopPropertySDKList);
                }

                // Take out the custom properties of stops
                List<CustomizedPropertySDK> customizedPropertySDKList = stop.getCustomizedPropertyList();
                // Determine if the stops attribute is empty
                if (null != customizedPropertySDKList && customizedPropertySDKList.size() > 0) {
                    List<ProcessStopCustomizedPropertySDK> processStopCustomizedPropertySDKList = new ArrayList<>();
                    // Attributes of loop stops
                    for (CustomizedPropertySDK customizedPropertySDK : customizedPropertySDKList) {
                        // isEmpty
                        if (null == customizedPropertySDK) {
                            continue;
                        }
                        ProcessStopCustomizedPropertySDK processStopCustomizedPropertySDK = new ProcessStopCustomizedPropertySDK();
                        // Copy customizedProperty information into processStopCustomizedProperty
                        BeanUtils.copyProperties(customizedPropertySDK, processStopCustomizedPropertySDK);
                        if (isAddId) {
                            processStopCustomizedPropertySDK.setId(UUIDUtils.getUUID32());
                        } else {
                            processStopCustomizedPropertySDK.setId(null);
                        }
                        // Associated foreign key
                        processStopCustomizedPropertySDKList.add(processStopCustomizedPropertySDK);
                    }
                    processStopSDK.setProcessStopCustomizedPropertyList(processStopCustomizedPropertySDKList);
                }
                break;
            }
            case DATASOURCE:{
                //本地的数据源,processStop存的DataSourceId和DataCenterName直接从DataSource获取,这里不必从配置文件中获取
                processStopSDK.setDataCenterId(stop.getDataCenterId());
                processStopSDK.setDataCenterName(stop.getDataCenterName());
                processStopSDK.setDataSourceId(stop.getFkDataSourceId());
                processStopSDK.setSourceType(SourceType.COLLABORATIVE_NETWORK);
                break;
            }
            case COMMONLYUSEDDATASOURCE:{
                //常用协同数据源,processStop存的DataSourceId和DataCenterName直接从CommonlyUsedDataSource获取,不必从缓存中查询
                processStopSDK.setDataCenterId(stop.getDataCenterId());
                processStopSDK.setDataCenterName(stop.getDataCenterName());
                processStopSDK.setDataSourceId(stop.getFkDataSourceId());
                break;
            }
            default:
                throw new Exception(MessageConfig.SCHEDULED_TYPE_OR_DATA_ERROR_MSG());
        }
        if (StopType.DATASOURCE == stop.getNodeType() || StopType.COMMONLYUSEDDATASOURCE == stop.getNodeType()){
            //如果是数据源或者协同数据源,dataCenterId/dataCenterName均不可为空,这里再校验一次
            if (StringUtils.isEmpty(processStopSDK.getDataCenterId()) || StringUtils.isEmpty(processStopSDK.getDataCenterName())){
                //这里再检查一次,如果是数据源,但两个字段有一个为空,就报错
                throw new Exception("dataCenterId or dataCenterName is null");
            }
        }
        return processStopSDK;
    }

    public static String processSDKToJson(ProcessSDK processSDK, String checkpoint, RunModeType runModeType, List<DatacenterInfo> datacenterInfoList) {
        Map<String, Object> flowVoMap = processSDKToMap(processSDK, checkpoint, runModeType, datacenterInfoList);
        return JsonUtils.toFormatJsonNoException(flowVoMap);
    }

    public static Map<String, Object> processSDKToMap(ProcessSDK processSDK, String checkpoint, RunModeType runModeType, List<DatacenterInfo> datacenterInfoList) {
        Map<String, Object> rtnMap = new HashMap<>();
        Map<String, Object> flowVoMap = new HashMap<>();
        flowVoMap.put("driverMemory", processSDK.getDriverMemory());
        flowVoMap.put("executorMemory", processSDK.getExecutorMemory());
        flowVoMap.put("executorCores", processSDK.getExecutorCores());
        flowVoMap.put("executorNumber", processSDK.getExecutorNumber());
        flowVoMap.put("name", processSDK.getName());
        flowVoMap.put("uuid", processSDK.getId());

        // all stops
        Map<String, ProcessStopSDK> stopsMap = new HashMap<>();

        List<Map<String, Object>> processStopMapList = new ArrayList<>();
        List<ProcessStopSDK> processStopSDKList = processSDK.getProcessStopList();
        for (ProcessStopSDK processStopSDK : processStopSDKList) {
            stopsMap.put(processStopSDK.getName(), processStopSDK);
        }

        // paths
        List<Map<String, Object>> thirdPathVoMapList = new ArrayList<>();
        List<ProcessPathSDK> processPathSDKList = processSDK.getProcessPathList();
        if (null != processPathSDKList && processPathSDKList.size() > 0) {
            for (ProcessPathSDK processPathSDK : processPathSDKList) {
                ProcessStopSDK fromProcessStopSDK = stopsMap.get(processPathSDK.getFrom());
                ProcessStopSDK toProcessStopSDK = stopsMap.get(processPathSDK.getTo());
                if (null == fromProcessStopSDK) {
                    fromProcessStopSDK = new ProcessStopSDK();
                }
                if (null == toProcessStopSDK) {
                    toProcessStopSDK = new ProcessStopSDK();
                }
                String to = (null != toProcessStopSDK.getName() ? toProcessStopSDK.getName() : "");
                String outport = (null != processPathSDK.getOutport() ? processPathSDK.getOutport() : "");
                String inport = (null != processPathSDK.getInport() ? processPathSDK.getInport() : "");
                String from = (null != fromProcessStopSDK.getName() ? fromProcessStopSDK.getName() : "");
                Map<String, Object> pathVoMap = new HashMap<>();
                pathVoMap.put("from", from);
                pathVoMap.put("outport", outport);
                pathVoMap.put("inport", inport);
                pathVoMap.put("to", to);
                if (PortType.ROUTE == fromProcessStopSDK.getOutPortType() && StringUtils.isNotBlank(outport)) {

                }
                if (PortType.ROUTE == toProcessStopSDK.getInPortType() && StringUtils.isNotBlank(inport)) {

                }
                thirdPathVoMapList.add(pathVoMap);
            }
        }
        flowVoMap.put("paths", thirdPathVoMapList);

        for (String stopPageId : stopsMap.keySet()) {
            ProcessStopSDK processStopSDK = stopsMap.get(stopPageId);
            Map<String, Object> thirdStopVo = new HashMap<>();
            thirdStopVo.put("uuid", processStopSDK.getId());
            thirdStopVo.put("name", processStopSDK.getName());
            //这里用三元运算符判断一下,如果是数据源的话,bundle是为空,但转为json时,如果value为null,此key就没了
            thirdStopVo.put("bundle", StringUtils.isEmpty(processStopSDK.getBundel())?"":processStopSDK.getBundel());
            thirdStopVo.put("dataCenter", "");
//            if (processStop.getIsDataSource()){
            if (StopType.DATASOURCE == processStopSDK.getNodeType() || StopType.COMMONLYUSEDDATASOURCE == processStopSDK.getNodeType()){
                thirdStopVo.put("dataSourceId",processStopSDK.getDataSourceId());
                //获取本地缓存的数据中心信息,根据dataCenterId,获取对应的web地址
                thirdStopVo.put("dataCenter", processStopSDK.getDataCenter());
                thirdStopVo.put("webAddress", processStopSDK.getWebAddress() + "/piflow-web");
                //无论是本地数据源还是常用协同数据源,flowJson中拼上"来源"字段
                thirdStopVo.put("sourceType",processStopSDK.getSourceType().name());
            }
            //如果是自定义算法且是python类型,拼上dataCenterId、dockerImagesName和python算法通用bundle和其他数据
            if(StopType.CUSTOMIZESTOP == processStopSDK.getNodeType() ){
                thirdStopVo.put("dataCenter", processStopSDK.getDataCenter()); //无论自定义组件是python还是scala,都分发到自己节点上运行,所以这里直接拼上自己的dataCenter
                if (ComponentFileType.PYTHON == processStopSDK.getComponentType()){
                    thirdStopVo.put("dataCenter", processStopSDK.getDataCenter());
                    Map<String, Object> pythonProperties = new HashMap<String, Object>();
                    String ymlContent = generateYmlContent(processStopSDK);
                    pythonProperties.put("inports",processStopSDK.getInports());
                    pythonProperties.put("outports",processStopSDK.getOutports());
                    pythonProperties.put("ymlContent",ymlContent);       //将yml的内容,拼到flowJson中发给server
                    thirdStopVo.put("properties", pythonProperties);
                    thirdStopVo.put("bundle", "cn.piflow.bundle.script.DockerExecute");
                }
            }

            // StopProperty
            List<ProcessStopPropertySDK> processStopPropertySDKList = processStopSDK.getProcessStopPropertyList();
            Map<String, Object> properties = (Map<String, Object>)thirdStopVo.get("properties");
            if (null == properties) {
                properties = new HashMap<String, Object>();
            }
            if (null != processStopPropertySDKList && processStopPropertySDKList.size() > 0) {
                for (ProcessStopPropertySDK processStopPropertySDK : processStopPropertySDKList) {
                    String name = processStopPropertySDK.getName();
                    if (StringUtils.isNotBlank(name)) {
                        String customValue2 = processStopPropertySDK.getCustomValue();
                        String customValue = (null != customValue2 ? customValue2 : "");
                        properties.put(name, customValue);
                    }
                }
            }
            thirdStopVo.put("properties", properties);  //这段属性代码需要放在下面这个if的上面,如果放在下面,python表合并后,会导致覆盖python常用算法的属性值(properties)

            //常用协同算法
            if (StopType.COMMONLYUSEDSTOP == processStopSDK.getNodeType()){
                //如果是常用scala算法,拿processStop中的mountId和心跳中的mount,parentMountId做比较,判断哪个server节点已经有此jar包了
                if (ComponentFileType.SCALA == processStopSDK.getComponentType()){
                    //获取心跳中的mount的jar包数据,进行判断
                    List<String> jarDataCenterAddress = new ArrayList<>();
                    List<String> jarMountIds = new ArrayList<>();
                    if (null == datacenterInfoList) {
                        datacenterInfoList = new ArrayList<>();
                    }
                    for (DatacenterInfo datacenterInfo : datacenterInfoList) {
                        List<StopsHubVo> stopsHubVoList = datacenterInfo.getStopsHubVoList();
                        if (stopsHubVoList !=null && stopsHubVoList.size()>0){
                            for (StopsHubVo stopsHubVo : stopsHubVoList) {
                                if (processStopSDK.getMountId().equals(stopsHubVo.getMountId()) || processStopSDK.getMountId().equals(stopsHubVo.getParentMountId())){
                                    //如果常用算法的mount和心跳中的mountId/parentMountId相同,就证明此节点有常用算法的jar包
                                    jarDataCenterAddress.add(datacenterInfo.getWebUrl());
                                    /**
                                     * 注意：这里无论是mountId相同还是MountId和ParentMountId相同,最后往flowJson中拼的都是数据中心自己的MountId
                                     * 比如：数据中心A在web端mount了一个jar包,A上面的mountId为123;假如后来C拉取过此jar包,C有一个自己的mountId为456,父mountId为123
                                     * B添加了A的一个协同算法,B中常用协同算法的mountId为123(来自于A的协同算法);此时心跳中有两个数据中心都有此jar包,
                                     * 判断到C数据中心时,是mountId=父mountId,但将来如果去C上面拉取此jar包,需要把C的mountId(456)传递给C
                                     */
                                    jarMountIds.add(stopsHubVo.getMountId());
                                    break;//这里可以直接跳出此次mount记录的循环了,继续走下一个数据中心的mount记录的循环;因为一个数据中心的mountId不可能重复,减少几次循环
                                }
                            }
                        }
                    }
                    //如果是常用协同算法,需要拼接的字段(jarDataCenterAddress,jarMountIds,这两个字段都是1~n个且一对一)
                    thirdStopVo.put("jarDataCenter",jarDataCenterAddress);
                    thirdStopVo.put("jarMountIds",jarMountIds);
                }else if (ComponentFileType.PYTHON == processStopSDK.getComponentType()){
                    //如果是常用python算法,拼上dockerImagesName和python算法通用bundle和其他数据
                    Map<String, Object> pythonProperties = new HashMap<String, Object>();
                    String ymlContent = generateYmlContent(processStopSDK);
                    pythonProperties.put("inports",processStopSDK.getInports());
                    pythonProperties.put("outports",processStopSDK.getOutports());
                    pythonProperties.put("ymlContent",ymlContent);       //将yml的内容,拼到flowJson中发给server
                    // docker-compose -f docker-compose.yml up
                    // 将来server端拉取的dockerYml会自动解压到/app目录下
                    // pythonProperties.put("shellString",String.format("docker-compose -f app/%s up",returnMap.get("ymlNam")));
                    thirdStopVo.put("properties", pythonProperties);
                    thirdStopVo.put("bundle", "cn.piflow.bundle.script.DockerExecute");
                }
            }

            // StopCustomizedProperty
            List<ProcessStopCustomizedPropertySDK> processStopCustomizedPropertySDKList = processStopSDK.getProcessStopCustomizedPropertyList();
            Map<String, Object> customizedProperties = new HashMap<String, Object>();
            if (null != processStopCustomizedPropertySDKList && processStopCustomizedPropertySDKList.size() > 0) {
                for (ProcessStopCustomizedPropertySDK processStopCustomizedPropertySDK : processStopCustomizedPropertySDKList) {
                    String name = processStopCustomizedPropertySDK.getName();
                    if (StringUtils.isNotBlank(name)) {
                        String customValue2 = processStopCustomizedPropertySDK.getCustomValue();
                        String customValue = (null != customValue2 ? customValue2 : "");
                        customizedProperties.put(name, customValue);
                    }
                }
            }
            thirdStopVo.put("customizedProperties", customizedProperties);

            processStopMapList.add(thirdStopVo);
        }
        flowVoMap.put("stops", processStopMapList);

        //checkpoint
        if (StringUtils.isNotBlank(checkpoint)) {
            flowVoMap.put("checkpoint", checkpoint);
        }
        if (RunModeType.DEBUG == runModeType) {
            flowVoMap.put("runMode", runModeType.getValue());
        }
        rtnMap.put("flow", flowVoMap);
        return rtnMap;
    }

	//生成docker执行需要的yml内容
    public static String generateYmlContent(ProcessStopSDK processStopSDK){
        long currentTimeMillis = System.currentTimeMillis();
        StringBuffer ymlContentSb = new StringBuffer();
        String ymlName = processStopSDK.getName() + "-" + currentTimeMillis;
        ymlContentSb.append("version: \"3\"" + System.lineSeparator());
        ymlContentSb.append("services:" + System.lineSeparator());
        ymlContentSb.append("  docker-python-demo:" + System.lineSeparator());
        //将来要执行的docker镜像：10.0.80.199/piflow-web:1.2，前面是中心仓库的ip，因为将来手动打docker镜像推到中心仓库时，需要把镜像名打成这种格式
        ymlContentSb.append("    image: \"" + processStopSDK.getDockerImagesName()+"\"" + System.lineSeparator());
        //2023-01-05:因为想把Python也进行分发,所以这里的参数不能再写本地的,改为可以替换的值,将来在哪个server提交任务,哪个sever去替换bigflow_extra_hosts
        //server端在bigflow_extra_hosts前自己拼了换行
        ymlContentSb.append("    extra_hosts:bigflow_extra_hosts" + System.lineSeparator());
        /*String hdfsClusterIpNameArr = SysParamsCache.HDFS_CLUSTER_IP_NAME_ARR;
        String[] hdfsClusterArr = hdfsClusterIpNameArr.split(",");
        for (String s : hdfsClusterArr) {
            ymlymlContentSb.append(String.format("      - \"%s\"", s) + System.lineSeparator());
        }*/
//        ymlContentSb.append("bigflow_extra_hosts"+System.lineSeparator());     //2023-01-05:因为想把Python也进行分发,所以这里的参数不能再写本地的,改为可以替换的值,将来在哪个server提交任务,哪个sever去替换bigflow_extra_hosts
        ymlContentSb.append("    hostname: " + ymlName + System.lineSeparator());
        ymlContentSb.append("    container_name: " + ymlName + System.lineSeparator());
        ymlContentSb.append("    environment:" + System.lineSeparator());
//        ymlymlContentSb.append(String.format("      - hdfs_url=%s", SysParamsCache.HDFS_URL) + System.lineSeparator());       //从配置文件中获取
        ymlContentSb.append("      - hdfs_url=bigflow_hdfs_url" + System.lineSeparator());       //2023-01-05:因为想把Python也进行分发,所以这里的参数不能再写本地的,改为可以替换的值,将来在哪个server提交任务,哪个sever去替换
        ymlContentSb.append("      - TZ=Asia/Shanghai" + System.lineSeparator());
        ymlContentSb.append("    volumes:" + System.lineSeparator());
        ymlContentSb.append("      - ../app:/app" + System.lineSeparator());
        //如果python有参数,拼上参数
        ymlContentSb.append("    command: python3 /pythonDir/" + processStopSDK.getBundel()+" " );
        if (processStopSDK.getProcessStopPropertyList() !=null && processStopSDK.getProcessStopPropertyList().size()>0){
            Collections.sort(processStopSDK.getProcessStopPropertyList());
            for (ProcessStopPropertySDK processStopPropertySDK : processStopSDK.getProcessStopPropertyList()) {
                ymlContentSb.append(processStopPropertySDK.getCustomValue()+" ");
            }
        }
        ymlContentSb.append(System.lineSeparator());
        ymlContentSb.append("    network_mode: bridge" + System.lineSeparator());
        return ymlContentSb.toString();
    }

}
