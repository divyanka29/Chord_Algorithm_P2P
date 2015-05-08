import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;


public class QueryResult {

	String searchRequest;
	String OriginalFilename;
	Double QueryTime;
	Integer HopCount;
	String FoundIP;
	String FoundPort;
	Boolean Found = false;
	
	public QueryResult(String searchRequest, String OriginalFilename, Boolean Found, Double QueryTime, Integer HopCount, String FoundIP, String FoundPort) throws UnsupportedEncodingException, NoSuchAlgorithmException {
		this.searchRequest=searchRequest;
		this.QueryTime=QueryTime;
		this.OriginalFilename = OriginalFilename;
		this.HopCount=HopCount;
		this.FoundIP = FoundIP;
		this.FoundPort=FoundPort;
		this.Found =Found;
	}
	
	
	
	
	
}
