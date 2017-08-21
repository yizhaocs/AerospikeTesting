package com.adara.newcache.servlet;

import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * curl "http://localhost:8080/webappservicewithservletexample/memcacheTesting?mode=read"
 * curl "http://localhost:8080/webappservicewithservletexample/memcacheTesting?mode=write"
 *
 */

public class MemcacheTestingServlet implements HttpRequestHandler {
    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String mode = req.getParameter("mode");


    }


    public void init() throws ServletException {
        System.out.println("[MemcacheTestingServlet.init]");
    }

    public void destroy() throws ServletException {
        System.out.println("[MemcacheTestingServlet.destroy]");
    }

}