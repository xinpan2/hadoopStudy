package com.xinpaninjava.server;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import com.xinpaninjava.service.LoginService;
import com.xinpaninjava.service.impl.LoginServiceImpl;

/**
 * in this class,we get the socket server by rpc and start the server to permit
 * the accession from the controller
 * 
 * measures:
 * 
 * first: get the builder
 * 
 * second:set the concrete info to the builder about address
 * 
 * third:obtain the server from builder
 * 
 * fourth:start the server and check the port we have assigned
 * 
 */
public class SocketServer {

	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		Builder builder = new RPC.Builder(new Configuration());

		builder.setBindAddress("xinpan2").setInstance(new LoginServiceImpl()).setPort(11111)
				.setProtocol(LoginService.class);

		Server server = builder.build();
		server.start();
	}

}
