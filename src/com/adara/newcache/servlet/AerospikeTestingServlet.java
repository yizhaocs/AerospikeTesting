package com.adara.newcache.servlet;

import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


/**
 * curl "http://localhost:8080/webappservicewithservletexample/aerospikeTesting?mode=read"
 * curl "http://localhost:8080/webappservicewithservletexample/aerospikeTesting?mode=write"
 *
 */


/**
 * @author YI ZHAO
 */
public class AerospikeTestingServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(AerospikeTestingServlet.class);

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String mode = req.getParameter("mode");


    }


    public void init() throws ServletException {
        System.out.println("[AerospikeTestingServlet.init]");
    }

    public void destroy() throws ServletException {
        System.out.println("[AerospikeTestingServlet.destroy]");
    }
}
