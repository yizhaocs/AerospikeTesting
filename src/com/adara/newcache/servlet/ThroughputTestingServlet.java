package com.adara.newcache.servlet;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.opinmind.ssc.cache.AerospikeService;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * scp /Users/yzhao/IdeaProjects/AerospikeTesting/dist/aerospiketesting.war manager:/home/yzhao
 * ssh manager
 * scp aerospiketesting.war qa-inweb1:/home/yzhao
 * ssh qa-inweb1
 * sudo cp aerospiketesting.war /opt/apache-tomcat/webapps/
 * sudo /sbin/service tomcat restart
 * tail -f /opt/apache-tomcat/logs/catalina.out
 * curl "http://localhost:8080/aerospiketesting/throughputtesting?mode=sync&start=0&end=2000&database=test&table=test1"
 * curl "http://localhost:8080/aerospiketesting/throughputtesting?mode=async&start=0&end=2000&database=test&table=test1"
 * curl "http://localhost:8080/aerospiketesting/throughputtesting?mode=async&start=0&end=2000&database=test&table=test1&allowConcurrentCommandsPerEventloop=40&eventLoopSize=4"
 */

public class ThroughputTestingServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(ThroughputTestingServlet.class);
    static String columnName1 = "column1";
    static String columnName2 = "column2";
    private AerospikeService aerospikeService;

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        System.out.println("[ThroughputTestingServlet.handleRequest]");

        String mode = req.getParameter("mode");
        int start = Integer.valueOf(req.getParameter("start"));
        int end = Integer.valueOf(req.getParameter("end"));
        String database = req.getParameter("database");
        String table = req.getParameter("table");
        int allowConcurrentCommandsPerEventloop = Integer.valueOf(req.getParameter("allowConcurrentCommandsPerEventloop"));
        int eventLoopSize = Integer.valueOf(req.getParameter("eventLoopSize"));
/*


        if(allowConcurrentCommandsPerEventloop != 0) {
            aerospikeService.setAllowConcurrentCommandsPerEventloop(allowConcurrentCommandsPerEventloop);

        }
        if(eventLoopSize != 0) {
            aerospikeService.setEventLoopSize(eventLoopSize);
        }


        // reload the config the async
        if(allowConcurrentCommandsPerEventloop != 0 || eventLoopSize != 0){
            aerospikeService.reloadConfig();
        }

*/

        int count = 0;
        long startTime = System.nanoTime();
        for (int i = start; i < end; i++) {
            Key row = new Key(database, table, i);
            Bin bin1 = new Bin(columnName1, 1);
            Bin bin2 = new Bin(columnName2, 2);

            if(mode.equals("sync")){
                aerospikeService.putRecord(null, row, bin1, bin2);
            }else if(mode.equals("async")){
                aerospikeService.putRecordAsync(null, row, bin1, bin2);
            }
            count++;
        }

        long endTime = System.nanoTime();

        long duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
        System.out.println("[ThroughputTestingServlet.handleRequest]: duration for write: total with " + duration + " milliseconds ,and per query:" + duration / (count) + " milliseconds,  count:" + count);
        log.info("[ThroughputTestingServlet.handleRequest]: duration for write: total with " + duration + " milliseconds ,and per query:" + duration / (count) + " milliseconds,  count:" + count);
    }


    public void init() throws ServletException {
        System.out.println("[ThroughputTestingServlet.init]");
    }

    public void destroy() throws ServletException {
        System.out.println("[ThroughputTestingServlet.destroy]");
    }

    public void setAerospikeService(AerospikeService aerospikeService) {
        this.aerospikeService = aerospikeService;
    }
}
