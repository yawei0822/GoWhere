package rpc;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class RpcHelper {
	// Writes a JSONArray to http response.
	public static void writeJsonArray(HttpServletResponse response, JSONArray array) throws IOException {
		PrintWriter out = response.getWriter();
		try {
			response.setContentType("application/json");
			response.setHeader("Access-Control-Allow-Origin", "*");
			out.print(array);
		} catch (Exception e) {
			e.printStackTrace();
		}
		out.close();
	}
    // Writes a JSONObject to http response.
	public static void writeJsonObject(HttpServletResponse response, JSONObject obj) throws IOException {
		PrintWriter out = response.getWriter();
		try {
			response.setContentType("application/json");
			response.setHeader("Access-Control-Allow-Origin", "*");
			out.print(obj);
		} catch (Exception e) {
			e.printStackTrace();
		}
		out.close();		
	}

}
