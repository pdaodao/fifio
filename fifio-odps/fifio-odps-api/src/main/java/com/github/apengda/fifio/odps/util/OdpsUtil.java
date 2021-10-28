package com.github.apengda.fifio.odps.util;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.github.apengda.fifio.jdbc.frame.DbInfo;

public class OdpsUtil {

    public static Odps initOdps(DbInfo dbInfo) {
        return initOdps(dbInfo.getUrl(), dbInfo.getDbName(), dbInfo.getUsername(), dbInfo.getPassword());
    }

    public static Odps initOdps(final String url, String project, String accessId, String accessKey) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.getRestClient().setConnectTimeout(10);
        odps.getRestClient().setReadTimeout(60);
        odps.getRestClient().setRetryTimes(2);
        odps.setDefaultProject(project);
        odps.setEndpoint(url);
        return odps;
    }

}
