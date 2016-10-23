import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {

	/* TODO: Add code to implement your backend storage */
	private static Map<String, BlockingQueue> map = new ConcurrentHashMap<String, BlockingQueue>();
	private static Map<String, DataUnit> data = new HashMap<String, DataUnit>();

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4096);
		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				String value = map.get("value");
				String consistency = map.get("consistency");
				Integer region = Integer.parseInt(map.get("region"));
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				/* TODO: Add code here to handle the put request
					 Remember to use the explicit timestamp if needed! */
				Thread t = new Thread(new Runnable() {
					public void run() {

						try {
							if (consistency.equals("strong")) {
								// Strong Consistency mode
								strongPUT(key, value, timestamp);
							} else if (consistency.equals("eventual")) {
								// Eventual Consistency mode
								eventualPUT(key, value, timestamp);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}

						// Generate response
						String response = "stored";
						// generateResponse("stored", req, "PUT");
						req.response().putHeader("Content-Type", "text/plain");
			            req.response().putHeader("Content-Length", String.valueOf(response.length()));
			            req.response().end(response);
			            req.response().close();
					}
				});
				t.start();
				//Do not remove this
				// req.response().end();
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				String consistency = map.get("consistency");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				/* TODO: Add code here to handle the get request
					 Remember that you may need to do some locking for this */

				Thread t = new Thread(new Runnable() {
					public void run() {

						String response = "";
						try {
							if (consistency.equals("strong")) {
								// Strong Consistency mode
								response = strongGET(key, timestamp);
							} else if (consistency.equals("eventual")) {
								// Eventual Consistency mode
								response = eventualGET(key, timestamp);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}

						// Generate Response
						// generateResponse(response, req, "GET");
						req.response().putHeader("Content-Type", "text/plain");
                        if (response != null)
                            req.response().putHeader("Content-Length",
                                                     String.valueOf(response.length()));
                        req.response().end(response);
                        req.response().close();
					}
				});
				t.start();
			}
		});

		// Clears this stored keys. Do not change this
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				/* TODO: Add code to here to flush your datastore. This is MANDATORY */
				map.clear();
				data.clear();

				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});

		// Handler for when the AHEAD is called
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap multi_map = req.params();
				String key = multi_map.get("key");
				final Long timestamp = Long.parseLong(multi_map.get("timestamp"));
				/* TODO: Add code to handle the signal here if you wish */

				// Update hashmap for blocking
				try {
					if (!map.containsKey(key)) {
						BlockingQueue<Long> queue = new PriorityBlockingQueue<Long>();
	            		queue.add(timestamp);
	            		map.put(key, queue);
					} else {
						map.get(key).add(timestamp);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});

		// Handler for when the COMPLETE is called
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				/* TODO: Add code to handle the signal here if you wish */
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
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
	 * Generate response for PUT and GET
	 */
	private void generateResponse(String response, final HttpServerRequest req, String mode) {
		
		if (mode.equals("PUT")) {
			req.response().putHeader("Content-Type", "text/plain");
            req.response().putHeader("Content-Length", String.valueOf(response.length()));
            req.response().end(response);
            req.response().close();
		} else if (mode.equals("GET")) {
			req.response().putHeader("Content-Type", "text/plain");
            if (response != null) {
            	req.response().putHeader("Content-Length", String.valueOf(response.length()));
            }
            req.response().end(response);
            req.response().close();
		}
	}


	/*
	 * PUT resources with strong consistency
	 */
	private void strongPUT(String key, String value, Long timestamp) {

		System.out.println("PUT key: " + key + "  timestamp: " + timestamp);
		
		BlockingQueue queue = map.get(key);

		if (queue == null) {
			System.out.println("1111111111");
		} else if (queue.peek() == null) {
			System.out.println("2222222222");
		}

		// Wait for its turn
		while (!queue.peek().equals(timestamp)) {
			// try {
			// 	System.out.println("PUT Waiting key: " + key + "  timestamp: " + timestamp);
			// 	// queue.wait();
			// } catch (Exception e) {
			// 	e.printStackTrace();
			// }
		}

		// Now update the data
		System.out.println("PUT Begin Executing key: " + key + "  timestamp: " + timestamp);

		DataUnit dataUnit = new DataUnit(value, timestamp);
		data.put(key, dataUnit);
		queue.poll();

		System.out.println("PUT Finished key: " + key + "  timestamp: " + timestamp);
	}


	/*
	 * PUT resources with eventual consistency
	 */
	private void eventualPUT(String key, String value, Long timestamp) {
		
		// If doesn't contains this key
		if (!data.containsKey(key) || data.get(key) == null || data.get(key).getValue() == null) {
			DataUnit dataUnit = new DataUnit(value, timestamp);
			data.put(key, dataUnit);
			return;
		}

		Long prev_time = data.get(key).getTimestamp();
		
		// Keep time order
		if (prev_time - timestamp < 0) {
			// Update data
			DataUnit dataUnit = new DataUnit(value, timestamp);
			data.put(key, dataUnit);
		}
	}


	/*
	 * GET resources with strong consistency
	 */
	private String strongGET(String key, Long timestamp) {
		System.out.println("GET key: " + key + "  timestamp: " + timestamp);

		// Add task to queue
		if (!map.containsKey(key)) {
			BlockingQueue<Long> pq = new PriorityBlockingQueue<Long>();
			pq.add(timestamp);
			map.put(key, pq);
		} else {
			map.get(key).add(timestamp);
		}

		// Judge top of queue
		// Wait for the quque
		BlockingQueue<Long> queue = map.get(key);
		while (!queue.peek().equals(timestamp)) {
			// try {
			// 	// System.out.println("GET Waiting key: " + key + "  timestamp: " + timestamp);
			// 	// queue.wait();
			// 	// Thread.sleep(SLEEP_INTEVAL);
			// } catch (Exception e) {
			// 	e.printStackTrace();
			// }
		}
		System.out.println("GET Begin Executing key: " + key + "  timestamp: " + timestamp);

		// Now this task's turn
		if (!data.containsKey(key) || data.get(key) == null || data.get(key).getValue() == null) {
			System.out.println("returned null");
			return "";
		}
		String response = "";
		response = data.get(key).getValue();
		System.out.println("response: " + response);
		queue.poll();
		System.out.println("GET Finished key: " + key + "  timestamp: " + timestamp);

		return response;
	}


	/*
	 * GET resources with eventual consistency
	 */
	private String eventualGET(String key, Long timestamp) {
		
		if (!data.containsKey(key) || data.get(key) == null || data.get(key).getValue() == null) {
			return "";
		}
		return data.get(key).getValue();
	}
}
