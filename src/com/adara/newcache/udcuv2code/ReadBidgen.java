/*
package com.adara.newcache.udcuv2code;

import com.opinmind.bidgen.CookieRouter;
import com.opinmind.ssc.CookieData;
import com.opinmind.ssc.cache.RemoteUserDataCacheImplV3;
import com.opinmind.ssc.cache.UserDataCacheFactory;

import java.util.ArrayList;
import java.util.List;

*/
/**
 * Created by yzhao on 7/14/17.
 *//*

public class ReadBidgen {
    public static RemoteUserDataCacheImplV3 getUserDataCacheBDB()
            throws NumberFormatException, Exception {

        CookieRouter cookieRouter = new CookieRouter();
        cookieRouter.setNodes("localhost:8080");

        List<String> configFileList = new ArrayList<String>();
        configFileList.add("/opt/opinmind/conf/common.properties");
        configFileList.add("/opt/opinmind/conf/local.properties");
        configFileList.add("/opt/opinmind/conf/bidgen.nodes.properties");
        configFileList.add("/opt/opinmind/conf/credentials/passwords.properties");
        cookieRouter.setConfigFile(configFileList);
        cookieRouter.init();


        int maxItemListLength = 10000000;
        String maxItemListLengthStr = "100000";
        if (maxItemListLengthStr != null && maxItemListLengthStr.length() > 0) {
            maxItemListLength = Integer.valueOf(maxItemListLengthStr);
        }

        RemoteUserDataCacheImplV3 userDataCache = null;

        userDataCache = (RemoteUserDataCacheImplV3) UserDataCacheFactory
                .createRemoteUserDataCache(cookieRouter,
                        maxItemListLength, true);
        userDataCache.setBidgenCacheNodes("localhost:8080");
        userDataCache.init();

        return userDataCache;
    }

    public static CookieData getCookieDataFromCookieId(String cookieId){
        RemoteUserDataCacheImplV3 userDataCacheBDB = null;

        try {
            userDataCacheBDB = ReadBidgen.getUserDataCacheBDB();
        }catch(Exception e){
            System.out.println();
            e.printStackTrace();
        }
        CookieData mCookieData = null;

        try {
            mCookieData = userDataCacheBDB.getCookieRawData(Long.valueOf(cookieId));
        }catch(Exception e){
            System.out.println("[getPutOperations_adara_prod error]");
            e.printStackTrace();
        }
        return mCookieData;
    }

}
*/
