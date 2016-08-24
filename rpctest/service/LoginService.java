package com.xinpaninjava.service;

/**
 * this interface aims to test the rpc framework to login user
 * 
 * @author xinpan
 *
 */
public interface LoginService {
	long versionID = 1L;

	public String login(String uname, String passwd);
}
