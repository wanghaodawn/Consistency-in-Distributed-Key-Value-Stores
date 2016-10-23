import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	// This integer variable tells you what region you are in
	// 1 for US-E, 2 for US-W, 3 for Singapore
	private static int region = KeyValueLib.region;

	// Default mode: Strongly consistent
	// Options: strong, eventual
	private static String consistencyType = "strong";

	// If the hashcode of a string has been calculated, then needn't to calculate it again
	private static Map<String, Integer> hashing = new HashMap<String, Integer>();
	private static Map<String, BlockingQueue> map = new ConcurrentHashMap<String, BlockingQueue>();

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances. Be sure to match the regions with their DNS!
	 * Do the same for the 3 Coordinators as well.
	 */
	private static final String coordinatorUSE = "ec2-54-88-78-187.compute-1.amazonaws.com";
	private static final String coordinatorUSW = "ec2-52-201-212-241.compute-1.amazonaws.com";
	private static final String coordinatorSING = "ec2-54-165-64-223.compute-1.amazonaws.com";

	private static final String dataCenterUSE = "ec2-52-90-250-105.compute-1.amazonaws.com";
	private static final String dataCenterUSW = "ec2-54-152-208-181.compute-1.amazonaws.com";
	private static final String dataCenterSING = "ec2-52-207-228-31.compute-1.amazonaws.com";

	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenterUSE, 1);
		KeyValueLib.dataCenters.put(dataCenterUSW, 2);
		KeyValueLib.dataCenters.put(dataCenterSING, 3);
		KeyValueLib.coordinators.put(coordinatorUSE, 1);
		KeyValueLib.coordinators.put(coordinatorUSW, 2);
		KeyValueLib.coordinators.put(coordinatorSING, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4096);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				final String time_string = map.get("timestamp");
				final Long timestamp = Long.parseLong(time_string);
				final String forwarded = map.get("forward");
				final String forwardedRegion = map.get("region");
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						/* TODO: Add code for PUT request handling here
						 * Each operation is handled in a new thread.
						 * Use of helper functions is highly recommended */

						try {
							if (consistencyType.equals("strong")) {
								// Strong Consistency mode
								strongPUT(key, value, time_string, forwarded);
							} else if (consistencyType.equals("eventual")) {
								// Eventual Consistency mode
								eventualPUT(key, value, time_string, forwarded);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String time_string = map.get("timestamp");
				final Long timestamp = Long.parseLong(time_string);
				
				Thread t = new Thread(new Runnable() {
					public void run() {
					/* TODO: Add code for GET requests handling here
					 * Each operation is handled in a new thread.
					 * Use of helper functions is highly recommended */
						
						String response = "0";
						try {
							if (consistencyType.equals("strong")) {
								// Strong Consistency mode
								response = strongGET(key, time_string);
							} else if (consistencyType.equals("eventual")) {
								// Eventual Consistency mode
								response = eventualGET(key, time_string);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						req.response().end(response);
					}
				});
				t.start();
			}
		});
		/* This endpoint is used by the grader to change the consistency level */
		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				consistencyType = map.get("consistency");
				req.response().end();
			}
		});
		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}

	/*
	 * send PUT request in strong consistency mode
	 */
	private static void strongPUT(String key, String value, String time_string, String forwarded) throws IOException {

		System.out.println("PUT key: " + key + "  timestamp: " + time_string);

		// Ensure the key is in the map
		if (!map.containsKey(key)) {
			BlockingQueue<String> pq = new PriorityBlockingQueue<String>();
			pq.add(time_string);
			map.put(key, pq);
		} else {
			map.get(key).add(time_string);
		}
		
		BlockingQueue queue = map.get(key);
		System.out.println("PUT Begin Executing key: " + key + "  timestamp: " + time_string);

		System.out.println("forwarded " + forwarded);

		if (forwarded != null) {
			queue.poll();
			KeyValueLib.PUT(dataCenterUSE,  key, value, time_string, consistencyType); 
			KeyValueLib.PUT(dataCenterUSW,  key, value, time_string, consistencyType);
			KeyValueLib.PUT(dataCenterSING, key, value, time_string, consistencyType);
		} else {
			int hashcode = HashCode(key);
			System.out.println("hashcode: " + hashcode + " region: " + region);

			if (hashcode != region) {
				System.out.println("ahead");
				KeyValueLib.AHEAD(key, time_string);
				switch (hashcode) {
					case 1:
						KeyValueLib.FORWARD(coordinatorUSE,  key, value, time_string);
						break;
					case 2:
						KeyValueLib.FORWARD(coordinatorUSW,  key, value, time_string);
						break;
					case 3:
						KeyValueLib.FORWARD(coordinatorSING, key, value, time_string);
						break;
				}
				queue.poll();
			} else {
				// Primarys
				KeyValueLib.AHEAD(key, time_string);
				queue.poll();
				KeyValueLib.PUT(dataCenterUSE,  key, value, time_string, consistencyType); 
				KeyValueLib.PUT(dataCenterUSW,  key, value, time_string, consistencyType);
				KeyValueLib.PUT(dataCenterSING, key, value, time_string, consistencyType);
			}
		}

		System.out.println("PUT Finished key: " + key + "  timestamp: " + time_string);
	}

	/*
	 * send GET request in strong consistency mode
	 */
	private static String strongGET(String key, String time_string) throws IOException {

		System.out.println("GET key: " + key + "  timestamp: " + time_string);

		// Ensure the key is in the map
		if (!map.containsKey(key)) {
			BlockingQueue<String> pq = new PriorityBlockingQueue<String>();
			pq.add(time_string);
			map.put(key, pq);
		} else {
			map.get(key).add(time_string);
		}

		BlockingQueue queue = map.get(key);
		while (!queue.peek().equals(time_string)) {
			// // Sleep
			// try {
			// 	System.out.println("GET Waiting key: " + key + "  timestamp: " + time_string);
			// 	queue.wait();
			// } catch (InterruptedException e) {
			// 	e.printStackTrace();
			// }
		}

		System.out.println("GET Begin Executing key: " + key + "  timestamp: " + time_string);

		// Send the GET request to the target datacenter
		String res = "";
		switch (region) {
			case 1:
				res = KeyValueLib.GET(dataCenterUSE,  key, time_string, consistencyType);
				break;
			case 2:
				res = KeyValueLib.GET(dataCenterUSW,  key, time_string, consistencyType);
				break;
			case 3:
				res = KeyValueLib.GET(dataCenterSING, key, time_string, consistencyType);
				break;
		}
		map.get(key).poll();

		System.out.println("GET Finished key: " + key + "  timestamp: " + time_string);
		return res;
	}
	
	/*
	 * send PUT request in eventual consistency mode
	 */
	private static void eventualPUT(String key, String value, String time_string, String forwarded) throws IOException {
	
		if (forwarded != null) {
			KeyValueLib.PUT(dataCenterUSE,  key, value, time_string, consistencyType); 
			KeyValueLib.PUT(dataCenterUSW,  key, value, time_string, consistencyType);
			KeyValueLib.PUT(dataCenterSING, key, value, time_string, consistencyType);
		} else {
			int hashcode = HashCode(key);

			if (hashcode != region) {
				switch (hashcode) {
					case 1:
						KeyValueLib.FORWARD(coordinatorUSE,  key, value, time_string);
						break;
					case 2:
						KeyValueLib.FORWARD(coordinatorUSW,  key, value, time_string);
						break;
					case 3:
						KeyValueLib.FORWARD(coordinatorSING, key, value, time_string);
						break;
				}
			} else {
				// Primarys
				KeyValueLib.PUT(dataCenterUSE,  key, value, time_string, consistencyType); 
				KeyValueLib.PUT(dataCenterUSW,  key, value, time_string, consistencyType);
				KeyValueLib.PUT(dataCenterSING, key, value, time_string, consistencyType);
			}
		}
	}

	/*
	 * send PUT request in eventual consistency mode
	 */
	private static String eventualGET(String key, String time_string) throws IOException {

		switch (region) {
			case 1:
				return KeyValueLib.GET(dataCenterUSE,  key, time_string, consistencyType);
			case 2:
				return KeyValueLib.GET(dataCenterUSW,  key, time_string, consistencyType);
			case 3:
				return KeyValueLib.GET(dataCenterSING, key, time_string, consistencyType);
		}
		return "";
	}

	/*
	 * Compute HashCode of a input String
	 */
	private static int HashCode(String key) {

		int result = 0;
		if (hashing.containsKey(key)) {
			// If has already computed this hashcode
			result = hashing.get(key);
		} else {
			// If this is a new key, compute it and then add it to the map
			for (int i = 0; i < key.length(); i++) {
				int temp = key.charAt(i) - 'a';
				switch (i % 3) {
					case 0:
						result += temp;
						break;
					case 1:
						result -= temp;
						break;
					case 2:
						result -= temp;
						break;
				}
			}
			// Ensure non-negative and a1 b2 c3
			result = ((result % 3) + 3) % 3 + 1;
			hashing.put(key, result);
		}

		System.out.println("key" + key + "    hashCode: " + result);
		return result;
	}
}
