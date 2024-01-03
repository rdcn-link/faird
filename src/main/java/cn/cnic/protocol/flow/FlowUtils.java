package cn.cnic.protocol.flow;

import cn.cnic.base.utils.UUIDUtils;

/**
 * @author yaxuan
 * @create 2023/11/20 15:52
 */
public class FlowUtils {

    public static String flowBeanToFlowJson(Flow flow) {
        try {
            ProcessSDK processSDK = ProcessSDKUtils.flowSDKToProcessSDK(flow, true);
            if (null == processSDK) {
                throw new Exception("Process Conversion Failed");
            }
            processSDK.setRunModeType(RunModeType.RUN);
            processSDK.setId(UUIDUtils.getUUID32());
            String flowJson = ProcessSDKUtils.processSDKToJson(processSDK, "", RunModeType.RUN, null);
            return flowJson;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
