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
 * scp aerospiketesting.war qa-inweb1:/home/yzhao
 * curl "http://localhost:8080/aerospiketesting/aerospiketesting?mode=write&start=0&end=2000&database=database1&table=set47"
 * curl "http://localhost:8080/aerospiketesting/aerospiketesting?mode=read&start=0&end=2000&database=database1&table=set47"
 *
 *
 */


/**
 * @author YI ZHAO
 */
public class AerospikeTestingServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(AerospikeTestingServlet.class);

    private AerospikeService aerospikeService;
    static String columnName1 = "id";
    static String columnName2 = "kv";

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String mode = req.getParameter("mode");
        int start =  Integer.valueOf(req.getParameter("start"));
        int end = Integer.valueOf(req.getParameter("end"));
        String database = req.getParameter("database");
        String table = req.getParameter("table");
        if(mode.equals("read")){
            long startTime = System.nanoTime();
            for(int i = start; i < end; i++){
                Key row = new Key(database, table, i);
                aerospikeService.getAllColumnsForRow(null, row);
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("duration for read: total with " + duration + " milliseconds ,and per query:" + duration/(end-start) + " milliseconds");
            log.info("duration for read: total with " + duration + " milliseconds ,and per query:" + duration/(end-start) + " milliseconds");
        }else if(mode.equals("write")){
            long startTime = System.nanoTime();
            for(int i = start; i < end; i++){
                Key row = new Key(database, table, i);
                Bin bin1 = new Bin(columnName1, i);
                Bin bin2 = new Bin(columnName2, i+1);
                aerospikeService.putColumnForRow(null, row, bin1, bin2);
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("duration for write: total with " + duration + " milliseconds ,and per query:" + duration/(end-start) + " milliseconds");
            log.info("duration for write: total with " + duration + " milliseconds ,and per query:" + duration/(end-start) + " milliseconds");
        }
    }


    public void init() throws ServletException {
        System.out.println("[AerospikeTestingServlet.init]");
    }

    public void destroy() throws ServletException {
        System.out.println("[AerospikeTestingServlet.destroy]");
    }

    public void setAerospikeService(AerospikeService aerospikeService) {
        this.aerospikeService = aerospikeService;
    }
}
