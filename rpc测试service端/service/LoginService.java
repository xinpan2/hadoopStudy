package com.xinpaninjava.service;

/**
 * this interface is used for bounding the behaviors of the impl
 * 
 * @author xinpan2
 *
 */
public interface LoginService {
	long versionID = 1L;

	public String login(String name, String passwd);
}
