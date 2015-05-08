import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

public class AuxFunctions {

	static Integer bit = 6;

	public static String getMsg(String rawMsg) {
		Integer lenMsg = rawMsg.length() + 4;
		String len = "0000" + lenMsg.toString();
		String len1 = len.substring(len.length() - 4, len.length());
		String finalMsg = len1 + rawMsg;

		return finalMsg;
	}

	public static ArrayList<Node> removeNode(ArrayList<Node> nodes, String IP, Integer Port) {
		// System.out.println(IP +":"+ Port);
		String check = IP + ":" + Port;
		// System.out.println(check.length());
		String IP_Port;
		for (int i = 0; i < nodes.size(); i++) {
			IP_Port = nodes.get(i).nodeIP + ":" + nodes.get(i).nodePort;
			IP_Port = IP_Port.trim();

			if (IP_Port.equalsIgnoreCase(check)) {
				nodes.remove(i);
			}

		}

		return nodes;

	}
	public static void TokenizeFilenames(ArrayList<FilesKeyTable> SelfFile, String Filename, Integer m) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		ArrayList<String> tokenlist = new ArrayList<String>();
		String input[]= Filename.split("_");
		for(int i=0; i<input.length; i++){
			SelfFile.add(new FilesKeyTable(input[i].trim(), AuxFunctions.HashingFiles(input[i].trim(), m), 
					Filename, null, null));
		}
		for(int i =0; i<input.length; i++){
			for(int j=i+1; j<input.length; j++){
				tokenlist.add(input[i].trim()+"_"+input[j].trim());
			}
		}
		for(int i=0;i<tokenlist.size(); i++){
			SelfFile.add(new FilesKeyTable(tokenlist.get(i), AuxFunctions.HashingFiles(tokenlist.get(i), m),
					Filename, null, null));
		}
	}

//	public static ArrayList<FilesKeyTable> removeDuplicateFiles(ArrayList<FilesKeyTable> files) {
//		String check;
//		HashSet hs = new HashSet();
//		ArrayList<FilesKeyTable> toreturn = new ArrayList<FilesKeyTable>();
//		ArrayList<String> temp = new ArrayList<String>();
//		for (int i = 0; i < files.size(); i++) {
//			check = new String(files.get(i).Filename + ":" + files.get(i).KeyValue + ":" + files.get(i).OriginalString);
//			temp.add(check);
//		}
//		hs.addAll(temp);
//		temp.clear();
//		temp.addAll(hs);
//		for(int i =0; i<temp.size(); i++){
//			for(int j=0; j<files.size(); j++){
//				String s = new String(files.get(j).Filename + ":" + files.get(j).KeyValue + ":" + files.get(j).OriginalString);
//				if(s.equalsIgnoreCase(temp.get(i))){
//					toreturn.add(files.get(j));
//					break;
//				}
//			}
//		}
//		return toreturn  ;
//	}

	public static ArrayList<FilesKeyTable> removeDuplicateFiles(ArrayList<FilesKeyTable> files) {
		files.trimToSize();
		TreeMap< Integer, FilesKeyTable> tree= new TreeMap< Integer, FilesKeyTable>();
		for (int i = 0; i < files.size(); i++) {
			int hash = (files.get(i).Filename + ":" + files.get(i).KeyValue + ":" + files.get(i).OriginalString).hashCode();
			tree.put(hash, files.get(i));
		}
		Set<Integer> keyset =tree.keySet();
		ArrayList<FilesKeyTable> toReturn = new ArrayList<FilesKeyTable>();
		for(Integer key: keyset){
			toReturn.add(tree.get(key));
			//tree.remove(key);
		}
		return toReturn;
	}
	
	public static ArrayList<Integer> Sortarray(ArrayList<Integer> nodeNumbers){
		ArrayList<Integer> outputArray = nodeNumbers;
		Collections.sort(outputArray);
		return outputArray;
	}

	public static String GenerateKey(String IP) throws UnsupportedEncodingException, NoSuchAlgorithmException{

		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(IP.getBytes());

		byte byteData[] = md.digest();

		//convert the byte to hex format method 2
		StringBuffer hexString = new StringBuffer();
		for (int i=0;i<byteData.length;i++) {
			String hex=Integer.toHexString(0xff & byteData[i]);
			if(hex.length()==1) hexString.append('0');
			hexString.append(hex);
		}
		//Get the last 8 hex digits

		String key = hexString.toString();
		key = key.substring(24);
		return key;
	}

	public static Integer String_to_Long(String key, Integer no_bits)
	{
		Long key_1 = Long.parseLong(key, 16);
		Integer final_key = new Double(key_1%(Math.pow(2, no_bits))).intValue();
		return final_key;
	}

	public static String HashingFiles(String filename, Integer NoBits) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		String FileKeyinHex = GenerateKey(filename.toLowerCase());
		String FileKeyValue = String_to_Long(FileKeyinHex, NoBits).toString();
		return FileKeyValue;	
	}

	public static ArrayList<Node> removeDuplicateNode(ArrayList<Node> nodes) {
		String check;
		String IP_Port;
		for (int i = 0; i < nodes.size(); i++) {
			check = new String(nodes.get(i).nodeIP + ":" + nodes.get(i).nodePort).trim();
			for (int j = i + 1; j < nodes.size(); j++) {
				IP_Port = new String(nodes.get(j).nodeIP + ":" + nodes.get(j).nodePort).trim();
				if (IP_Port.equalsIgnoreCase(check)) {
					nodes.remove(j);
					j--;
				}
			}
		}

		return nodes;

	}

	public static Boolean checkIfSelfNode(String IP_self, Integer Port_self, String IP, Integer Port) {
		String check = new String(IP + ":" + Port).trim();
		String IP_Port = new String(IP_self + ":" + Port_self).trim();

		if (IP_Port.equalsIgnoreCase(check))
			return true;
		else
			return false;
	}

}

