package com.xinpaninjava.service.impl;

import com.xinpaninjava.service.LoginService;

/**
 * specify the login method with just simple return
 */
public class LoginServiceImpl implements LoginService {

	@Override
	public String login(String name, String passwd) {
		return "hello " + name;
	}

}
