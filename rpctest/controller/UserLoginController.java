package com.xinpaninjava.controller;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.xinpaninjava.service.LoginService;

public class UserLoginController {
	public static void main(String[] args) throws IOException {
		LoginService loginService = RPC.getProxy(LoginService.class, 1L, new InetSocketAddress("xinpan", 11111),
				new Configuration());
		String res = loginService.login("xinpan", "123");
		System.out.println(res);
	}
}
