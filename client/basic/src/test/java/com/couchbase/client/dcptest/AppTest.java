package com.couchbase.client.dcptest;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
	protected void setUp(){
	}
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public AppTest( String testName )
	{
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite( AppTest.class );
	}

	// test method to add two values
	public void testJSON(){
		String js = "{\"result\":{\"Ips\":{\"/couchbase-a-0\":\"172.17.0.4\",\"/couchbase-a-1\":\"172.17.0.3\",\"/couchbase-a-2\":\"172.17.0.5\",\"/couchbase-a-3\":\"172.17.0.2\"}},\"error\":null,\"id\":\"1\"}";
		String strIp = "";
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readValue(js, JsonNode.class);
			JsonNode resultNode = node.get("result");
			JsonNode ipsNode = resultNode.get("Ips");
			JsonNode ip = ipsNode.get("/couchbase-a-1");
			strIp = ip.asText();
		} catch (Exception e) {
			System.out.printf("Exception:%s\n", e.toString());
			e.printStackTrace();
		}
		assertEquals("172.17.0.3", strIp);

	}

	protected void tearDown() {
	}
}
