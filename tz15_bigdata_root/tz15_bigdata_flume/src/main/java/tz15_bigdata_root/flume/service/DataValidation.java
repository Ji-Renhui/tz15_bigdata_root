package tz15_bigdata_root.flume.service;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import tz15_bigdata_root.flume.constant.ErrorMapFields;
import tz15_bigdata_root.flume.constant.MapFields;
import tz15_bigdata_root.flume.interceptor.ELKInterceptor;
import tz15_bigdata_root.flume.regex.Validation;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-04 20:01
 */
public class DataValidation {
    private static final Logger LOG = Logger.getLogger(ELKInterceptor.class);
    private static final String USERNAME=ErrorMapFields.USERNAME;

    private static final String SJHM=ErrorMapFields.SJHM;
    private static final String SJHM_ERROR=ErrorMapFields.SJHM_ERROR;
    private static final String SJHM_ERRORCODE=ErrorMapFields.SJHM_ERRORCODE;

    private static final String QQ=ErrorMapFields.QQ;
    private static final String QQ_ERROR=ErrorMapFields.QQ_ERROR;
    private static final String QQ_ERRORCODE=ErrorMapFields.QQ_ERRORCODE;

    private static final String IMSI=ErrorMapFields.IMSI;
    private static final String IMSI_ERROR=ErrorMapFields.IMSI_ERROR;
    private static final String IMSI_ERRORCODE=ErrorMapFields.IMSI_ERRORCODE;

    private static final String IMEI=ErrorMapFields.IMEI;
    private static final String IMEI_ERROR=ErrorMapFields.IMEI_ERROR;
    private static final String IMEI_ERRORCODE=ErrorMapFields.IMEI_ERRORCODE;

    private static final String MAC=ErrorMapFields.MAC;
    private static final String CLIENTMAC=ErrorMapFields.CLIENTMAC;
    private static final String STATIONMAC=ErrorMapFields.STATIONMAC;
    private static final String BSSID=ErrorMapFields.BSSID;
    private static final String MAC_ERROR=ErrorMapFields.MAC_ERROR;
    private static final String MAC_ERRORCODE=ErrorMapFields.MAC_ERRORCODE;

    private static final String DEVICENUM=ErrorMapFields.DEVICENUM;
    private static final String DEVICENUM_ERROR=ErrorMapFields.DEVICENUM_ERROR;
    private static final String DEVICENUM_ERRORCODE=ErrorMapFields.DEVICENUM_ERRORCODE;

    private static final String CAPTURETIME=ErrorMapFields.CAPTURETIME;
    private static final String CAPTURETIME_ERROR=ErrorMapFields.CAPTURETIME_ERROR;
    private static final String CAPTURETIME_ERRORCODE=ErrorMapFields.CAPTURETIME_ERRORCODE;


    private static final String EMAIL=ErrorMapFields.EMAIL;
    private static final String EMAIL_ERROR=ErrorMapFields.EMAIL_ERROR;
    private static final String EMAIL_ERRORCODE=ErrorMapFields.EMAIL_ERRORCODE;

    private static final String AUTH_TYPE=ErrorMapFields.AUTH_TYPE;
    private static final String AUTH_TYPE_ERROR=ErrorMapFields.AUTH_TYPE_ERROR;
    private static final String AUTH_TYPE_ERRORCODE=ErrorMapFields.AUTH_TYPE_ERRORCODE;

    private static final String FIRM_CODE=ErrorMapFields.FIRM_CODE;
    private static final String FIRM_CODE_ERROR=ErrorMapFields.FIRM_CODE_ERROR;
    private static final String FIRM_CODE_ERRORCODE=ErrorMapFields.FIRM_CODE_ERRORCODE;

    private static final String STARTTIME=ErrorMapFields.STARTTIME;
    private static final String STARTTIME_ERROR=ErrorMapFields.STARTTIME_ERROR;
    private static final String STARTTIME_ERRORCODE=ErrorMapFields.STARTTIME_ERRORCODE;
    private static final String ENDTIME=ErrorMapFields.ENDTIME;
    private static final String ENDTIME_ERROR=ErrorMapFields.ENDTIME_ERROR;
    private static final String ENDTIME_ERRORCODE=ErrorMapFields.ENDTIME_ERRORCODE;


    private static final String LOGINTIME=ErrorMapFields.LOGINTIME;
    private static final String LOGINTIME_ERROR=ErrorMapFields.LOGINTIME_ERROR;
    private static final String LOGINTIME_ERRORCODE=ErrorMapFields.LOGINTIME_ERRORCODE;
    private static final String LOGOUTTIME=ErrorMapFields.LOGOUTTIME;
    private static final String LOGOUTTIME_ERROR=ErrorMapFields.LOGOUTTIME_ERROR;
    private static final String LOGOUTTIME_ERRORCODE=ErrorMapFields.LOGOUTTIME_ERRORCODE;

    public static Map<String,Object> dataValidation(Map<String,String> map){
        //????????????
        //?????????????????????????????????
        if(map == null){
            return null;
        }
        //??????????????????????????????map
        Map<String,Object> errorMap = new HashMap<>();
        //??????????????????
        sjhmValidation(map,errorMap);
        //??????mac ??????mac???????????????????????????????????????
        macValidation(map,errorMap);
        //?????????

        //TODO ???????????????
        //TODO ??????????????????
        //TODO ????????????????????????
        //TODO ????????????  ???????????????????????????????????????
        //TODO ?????????
        return errorMap;
    }

    /**
     * ??????????????????
     * @param map
     * @param errorMap
     */
    public static void sjhmValidation(Map<String,String> map,Map<String,Object> errorMap){
        //phone
        if(map.containsKey(MapFields.PHONE)){
            String sjhm = map.get(MapFields.PHONE);
            //????????????  ???????????????
            boolean isMobile = Validation.isMobile(sjhm);
            if(!isMobile){
                LOG.error("===????????????????????????????????????"+sjhm);
                errorMap.put(SJHM,sjhm);
                errorMap.put(SJHM_ERROR,SJHM_ERRORCODE);
            }
        }
    }

    /**
     * MAC??????
     * @param map
     * @param errorMap
     */
    public static void macValidation(Map<String,String> map,Map<String,Object> errorMap){
        //phone
        if(map.containsKey(MapFields.PHONE_MAC)){
            String mac = map.get(MapFields.PHONE_MAC);
            //????????????  ???????????????
            if(StringUtils.isNotBlank(mac)){
                boolean bool = Validation.isMac(mac);
                if(!bool){
                    errorMap.put(MAC,mac);
                    errorMap.put(MAC_ERROR,MAC_ERRORCODE);
                }
            }else{
                LOG.error("MAC??????");
                errorMap.put(MAC,mac);
                errorMap.put(MAC_ERROR,MAC_ERRORCODE);
            }
        }
    }




}
