import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class NodeServer {

	//Initialize global variables

	Node curNode;
	Integer m = 15;												//No of Bits 
	ArrayList<Node> PeerDetails = new ArrayList<Node>();
	ArrayList<FingerTable> FingerTable = new ArrayList<FingerTable>();
	ArrayList<Integer> NodeNumbers = new ArrayList<Integer>();	
	ArrayList<String> Successor = new ArrayList<String>();
	volatile ArrayList<FilesKeyTable> FilesAsSuccessor = new ArrayList<FilesKeyTable>();
	ArrayList<FilesKeyTable> SelfFileTable = new ArrayList<FilesKeyTable>(); 
	String BootstrapIP = "";
	Integer BootstrapPort = 0;
	static Integer Self_Port;
	Socket sock_tcp;
	String Self_key = "";
	Integer Self_key_long;
	Boolean unregOK = false;
	volatile Boolean ADDFlag = false; 
	static int upfinokcounter =0;
	static int upfinleave = 0;
	static int numOfAddSent = 0;
	static int numOfADDOKrecd = 0;
	volatile int numOfGetkey=0;
	String Predeccessor = null;
	String SuccessorForSelfNode = null;
	static Integer numberOfMsgs = 0;

	ArrayList<String> queryList = new ArrayList<String>();
	ArrayList<String> HashOfQueryList = new ArrayList<String>();
	String resultFile = null;
	Double StartQueryTime = (double) 0;
	Double EndQueryTime = (double) 0;
	ArrayList<QueryResult> queryresult= new ArrayList<QueryResult>();


	public NodeServer() throws IOException, NoSuchAlgorithmException {

		try {

			//Generate Hash key for self IP

			Self_key = AuxFunctions.GenerateKey(InetAddress.getLocalHost().getHostAddress());
			Self_key_long = AuxFunctions.String_to_Long(Self_key, m);

			//assign local IP and Argument port to Current Node

			curNode = new Node(InetAddress.getLocalHost().getHostAddress(), Self_Port, Self_key, Self_key_long);

			// Set random files for current node

			//curNode.setFiles();
			//for(Integer i =0; i<curNode.filenames.size(); i++){
			//	SelfFileTable.add(new FilesKeyTable(curNode.filenames.get(i), 
			//			AuxFunctions.HashingFiles(curNode.filenames.get(i), m), curNode.filenames.get(i), 
			//			InetAddress.getLocalHost().getHostAddress().toString(), Self_Port.toString()));
			//}
			queryresult.clear();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	// Function to connect to Bootstrap server
	public void connect_to_bootstrap(NodeServer nodeServ) throws IOException {


		// IP and Port of Bootstrap server
		InetAddress BS_IP = InetAddress.getByName(nodeServ.BootstrapIP);
		int BS_port = nodeServ.BootstrapPort;
		Socket cliSock = new Socket(BS_IP, BS_port);
		// Connection request
		String connect_to_bs_1 = " REG " + curNode.nodeIP + " " + curNode.nodePort + " " + curNode.nodeNumberKey + "\n";
		String connectBS = AuxFunctions.getMsg(connect_to_bs_1);
		// Send registration request to Bootstrap Server
		byte[] msg = connectBS.getBytes();
		DataOutputStream writer = new DataOutputStream(cliSock.getOutputStream());
		writer.write(msg);


		System.out.println();
		System.out.println("Sent Msg: " + connectBS);
		System.out.println("To: " + cliSock.getInetAddress() + " " + cliSock.getPort());

		// Receive registration response from BootStrap Server
		InputStreamReader recv_data = new InputStreamReader(cliSock.getInputStream());
		BufferedReader reader = new BufferedReader(recv_data);
		String read = reader.readLine();
		System.out.println();
		System.out.println("Rcvd Msg: " + read);
		System.out.println("From: " + cliSock.getInetAddress() + " " + cliSock.getPort());
		// Decoding the registration response
		String input[] = read.split(" ");
		String no_peers = input[2].trim();
		cliSock.close();
		if (no_peers.equalsIgnoreCase("0")) {
			//	 System.out.println("Request is successful. You are the first node in the network");
		} else if (no_peers.equalsIgnoreCase("9999")) {
			System.out.println("Failed, Some Error in Command");
		} else if (no_peers.equalsIgnoreCase("9998")) {
			System.out.println("Failed, Already registered to you, first unregister");
		} else if (no_peers.equalsIgnoreCase("9997")) {
			System.out.println("Failed, can't register, BS full");
		} else {
			nodeServ.Predeccessor=nodeServ.Self_key_long.toString();
			StringTokenizer token = new StringTokenizer(read, " ");
			token.nextToken();
			token.nextToken();
			token.nextToken();

			//get peer details
			String s = read.substring(12);
			String r[] = s.split(" ");
			for (Integer i = 0; i< Integer.parseInt(no_peers); i++){
				Node peer = null;
				try {
					peer = new Node(token.nextToken().trim(), Integer.parseInt(token.nextToken().trim()), 
							token.nextToken().trim(), AuxFunctions.String_to_Long(r[3*(i+1)].trim(), nodeServ.m));
				} catch (NumberFormatException | NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				nodeServ.NodeNumbers.add(AuxFunctions.String_to_Long(r[3*(i+1)], nodeServ.m));
				nodeServ.PeerDetails.add(peer);
			}
		}

		nodeServ.NodeNumbers = AuxFunctions.Sortarray(nodeServ.NodeNumbers);

		/*Detect Predecessor : Can be displayed by entering "predecessor" in terminal*/

		if(nodeServ.NodeNumbers.get(0).toString().equalsIgnoreCase(nodeServ.Self_key_long.toString())){
			if(nodeServ.NodeNumbers.size()==1){
				nodeServ.Predeccessor=nodeServ.NodeNumbers.get(0).toString();
			}
			else{
				nodeServ.Predeccessor=nodeServ.NodeNumbers.get(nodeServ.NodeNumbers.size()-1).toString();
			}
		}
		else{
			for(Integer i = 1; i<=nodeServ.NodeNumbers.size()-1; i++){
				if(nodeServ.NodeNumbers.get(i).toString().equalsIgnoreCase(nodeServ.Self_key_long.toString())){
					nodeServ.Predeccessor=nodeServ.NodeNumbers.get(i-1).toString();
				}

			}

		}

		/*Detect Successor : Can be displayed by entering "successor" in terminal*/ 

		if(nodeServ.Self_key_long.toString().equalsIgnoreCase
				(nodeServ.NodeNumbers.get(nodeServ.NodeNumbers.size()-1).toString())){
			nodeServ.SuccessorForSelfNode = nodeServ.NodeNumbers.get(0).toString(); 
		}
		else{
			for(Integer i=0; i<nodeServ.PeerDetails.size(); i++){
				if(nodeServ.Self_key_long.toString().equalsIgnoreCase
						(nodeServ.NodeNumbers.get(i).toString())){
					nodeServ.SuccessorForSelfNode=nodeServ.NodeNumbers.get(i+1).toString();
				}
			}
		}

		/*create Finger Table with available peer from BootStrap : Can be displayed by entering "fingertable" in terminal*/

		try {
			nodeServ.createFingerTable(nodeServ, nodeServ.Self_key_long, nodeServ.m);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*Add self file table details : can be displayed by entering "files" in terminal*/

		ArrayList<FilesKeyTable> temp = new ArrayList<FilesKeyTable>();
		for(Integer i = 0; i< nodeServ.SelfFileTable.size(); i++){
			temp.add(new FilesKeyTable(nodeServ.SelfFileTable.get(i).Filename,
					nodeServ.SelfFileTable.get(i).KeyValue, nodeServ.SelfFileTable.get(i).Filename, nodeServ.curNode.nodeIP, nodeServ.curNode.nodePort.toString()));
		}
		nodeServ.SelfFileTable.clear();
		for(Integer i =0; i<temp.size(); i++){
			nodeServ.SelfFileTable.add(temp.get(i));
		}
		temp.clear();

		/*Tokenize and hash filenames*/

		ArrayList<FilesKeyTable> tem = new ArrayList<FilesKeyTable>();
		for(int i =0; i<nodeServ.SelfFileTable.size(); i++){
			try {
				AuxFunctions.TokenizeFilenames(tem, nodeServ.SelfFileTable.get(i).Filename, nodeServ.m);
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/*Update self file table with tokenized filenames*/

		for(int i =0; i<tem.size(); i++){
			tem.get(i).IP = nodeServ.curNode.nodeIP;
			tem.get(i).Port = nodeServ.curNode.nodePort.toString();
			nodeServ.SelfFileTable.add(tem.get(i));
		}

		/*Add relevant files to File as Successor Array : Can be displayed by entering "filesassuccessor"*/

		for(Integer i =0; i<nodeServ.SelfFileTable.size(); i++){
			if(nodeServ.PeerDetails.size()==0){
				nodeServ.FilesAsSuccessor.add(new FilesKeyTable(nodeServ.SelfFileTable.get(i).Filename, 
						nodeServ.SelfFileTable.get(i).KeyValue, nodeServ.SelfFileTable.get(i).OriginalString, nodeServ.SelfFileTable.get(i).IP, nodeServ.SelfFileTable.get(i).Port));
			}
			else{
				if(nodeServ.Self_key_long> Integer.parseInt(nodeServ.Predeccessor) && Integer.parseInt(nodeServ.SelfFileTable.get(i).KeyValue) > Integer.parseInt(nodeServ.Predeccessor)
						&& Integer.parseInt(nodeServ.SelfFileTable.get(i).KeyValue)<= nodeServ.Self_key_long){
					nodeServ.FilesAsSuccessor.add(new FilesKeyTable(nodeServ.SelfFileTable.get(i).Filename, 
							nodeServ.SelfFileTable.get(i).KeyValue, nodeServ.SelfFileTable.get(i).OriginalString, nodeServ.SelfFileTable.get(i).IP, nodeServ.SelfFileTable.get(i).Port));
				}
				else if(nodeServ.Self_key_long < Integer.parseInt(nodeServ.Predeccessor) && 
						Integer.parseInt(nodeServ.SelfFileTable.get(i).KeyValue) <= nodeServ.Self_key_long){
					nodeServ.FilesAsSuccessor.add(new FilesKeyTable(nodeServ.SelfFileTable.get(i).Filename, 
							nodeServ.SelfFileTable.get(i).KeyValue, nodeServ.SelfFileTable.get(i).OriginalString, nodeServ.SelfFileTable.get(i).IP, nodeServ.SelfFileTable.get(i).Port));
				}
				else if(nodeServ.Self_key_long < Integer.parseInt(nodeServ.Predeccessor) && Integer.parseInt(nodeServ.SelfFileTable.get(i).KeyValue) > Integer.parseInt(nodeServ.Predeccessor)){
					nodeServ.FilesAsSuccessor.add(new FilesKeyTable(nodeServ.SelfFileTable.get(i).Filename, 
							nodeServ.SelfFileTable.get(i).KeyValue, nodeServ.SelfFileTable.get(i).OriginalString, nodeServ.SelfFileTable.get(i).IP, nodeServ.SelfFileTable.get(i).Port));
				}
			}
		}
	}
// Unregister request handler
	
	public void UnregWithBootstrap(NodeServer NodeServ){
		try {
			Socket unSock;
			InetAddress BS_IP;
			int BS_port = NodeServ.BootstrapPort;
			BS_IP = InetAddress.getByName(NodeServ.BootstrapIP);
			unSock = new Socket(BS_IP, BS_port);
			String connect_to_bs_1 = " UNREG " + curNode.nodeNumberKey;
			String connectBS = AuxFunctions.getMsg(connect_to_bs_1);
			PrintWriter writer;
			
			//send UNREG Request
			writer = new PrintWriter(unSock.getOutputStream(),true);
			writer.println(connectBS);
			System.out.println();
			System.out.println("Sent Msg: " + connectBS);
			System.out.println("To: " + unSock.getInetAddress() + " " + unSock.getPort());

			InputStreamReader recv_data = new InputStreamReader(unSock.getInputStream());
			BufferedReader reader = new BufferedReader(recv_data);
			String read = reader.readLine();
			System.out.println();
			System.out.println("Rcvd Msg: " + read);
			System.out.println("From: " + unSock.getInetAddress() + " " + unSock.getPort());
			// Decoding the registration response
			unSock.close();
			
			//send update fingertable request to peers
			String UpdateFinger = " UPFIN 1 " + NodeServ.curNode.nodeIP + " " + NodeServ.curNode.nodePort + 
					" " + NodeServ.curNode.nodeNumberKey;
			UpdateFinger = AuxFunctions.getMsg(UpdateFinger);

			for(int i=0; i<NodeServ.PeerDetails.size(); i++){
				unSock = new Socket(InetAddress.getByName(NodeServ.PeerDetails.get(i).nodeIP), NodeServ.PeerDetails.get(i).nodePort);
				PrintWriter writer1;
				writer1 = new PrintWriter(unSock.getOutputStream(),true);
				writer1.println(UpdateFinger);
				System.out.println();
				System.out.println("Sent Msg: " + UpdateFinger);
				System.out.println("To: " + unSock.getInetAddress() + " " + unSock.getPort());


			}
			unSock.close();
			
			//give keys to successor
			int noGiveKey=0;
			String Givekey;
			
			Givekey = NodeServ.FilesAsSuccessor.get(0).Filename + " " + NodeServ.FilesAsSuccessor.get(0).OriginalString 
					+ " " + NodeServ.FilesAsSuccessor.get(0).KeyValue + " " 
					+ NodeServ.FilesAsSuccessor.get(0).IP + " " + NodeServ.FilesAsSuccessor.get(0).Port + " ";
			noGiveKey++;
			for(int i=0; i<NodeServ.FilesAsSuccessor.size(); i++){
				Givekey = NodeServ.FilesAsSuccessor.get(i).Filename + " " + 
			NodeServ.FilesAsSuccessor.get(i).OriginalString + " " + NodeServ.FilesAsSuccessor.get(i).KeyValue + " "
			 +  NodeServ.FilesAsSuccessor.get(i).IP + " " + NodeServ.FilesAsSuccessor.get(i).Port + " ";
				noGiveKey++;
			}
			
			Givekey = " " + noGiveKey + " " + Givekey;
			Givekey = AuxFunctions.getMsg(Givekey);

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	//function to update peer details
	public void UpdatePeerDetailsEnter(NodeServer Nodeserv, String IP, String Port, String Key){
		try {
			//get hash of peer key
			Integer key_long = AuxFunctions.String_to_Long(Key, Nodeserv.m);
			Integer port = Integer.parseInt(Port);
			Node add_node = new Node(IP, port, Key, key_long);
			
			//add IP and POrt of Peer
			Nodeserv.PeerDetails.add(add_node);
			Nodeserv.NodeNumbers.clear();
			Nodeserv.NodeNumbers.add(Nodeserv.Self_key_long);

			for(Integer i = 0; i<Nodeserv.PeerDetails.size(); i++){
				Nodeserv.NodeNumbers.add(Nodeserv.PeerDetails.get(i).nodeNumberKeyInLong);
			}
			
			//update predecessor and successor
			Nodeserv.NodeNumbers = AuxFunctions.Sortarray(Nodeserv.NodeNumbers);
			if(Nodeserv.NodeNumbers.get(0).toString().equalsIgnoreCase(Nodeserv.Self_key_long.toString())){
				if(Nodeserv.NodeNumbers.size()==1){
					Nodeserv.Predeccessor=Nodeserv.NodeNumbers.get(0).toString();
				}
				else{
					Nodeserv.Predeccessor=Nodeserv.NodeNumbers.get(Nodeserv.NodeNumbers.size()-1).toString();
				}
			}
			else{
				for(Integer i = 1; i<=Nodeserv.NodeNumbers.size()-1; i++){
					if(Nodeserv.NodeNumbers.get(i).toString().equalsIgnoreCase(Nodeserv.Self_key_long.toString())){
						Nodeserv.Predeccessor=Nodeserv.NodeNumbers.get(i-1).toString();
					}

				}

			}
			if(Nodeserv.Self_key_long.toString().equalsIgnoreCase
					(Nodeserv.NodeNumbers.get(Nodeserv.NodeNumbers.size()-1).toString())){
				Nodeserv.SuccessorForSelfNode = Nodeserv.NodeNumbers.get(0).toString(); 
			}
			else{
				for(Integer i=0; i<Nodeserv.PeerDetails.size(); i++){
					if(Nodeserv.Self_key_long.toString().equalsIgnoreCase
							(Nodeserv.NodeNumbers.get(i).toString())){
						Nodeserv.SuccessorForSelfNode=Nodeserv.NodeNumbers.get(i+1).toString();
					}
				}
			}
			Nodeserv.FingerTable.clear();
			Nodeserv.createFingerTable(Nodeserv, Nodeserv.Self_key_long, Nodeserv.m);
		} 
		catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch blockAdd
			e.printStackTrace();
		} 
		catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	//function to update peer details when a node leaves
	public void UpdatePeerDetailsLeave(NodeServer Nodeserv, String IP, String Port, String Key) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		Integer key_long = AuxFunctions.String_to_Long(Key, Nodeserv.m);
		for(int i=0; i<Nodeserv.NodeNumbers.size(); i++){
			if(key_long.intValue()==Nodeserv.NodeNumbers.get(i).intValue()){
				Nodeserv.NodeNumbers.remove(i);
			}
		}
		
		//remove from peer details
		for(int i=0; i< Nodeserv.PeerDetails.size(); i++){
			if(IP.equalsIgnoreCase(Nodeserv.PeerDetails.get(i).nodeIP) && Port.equals(Nodeserv.PeerDetails.get(i).nodePort.toString())
					&& key_long.toString().equalsIgnoreCase(Nodeserv.PeerDetails.get(i).nodeNumberKeyInLong.toString())){
				Nodeserv.PeerDetails.remove(i);
			}
		}

		Nodeserv.NodeNumbers = AuxFunctions.Sortarray(Nodeserv.NodeNumbers);
		
		//update predecessor and successor
		if(Nodeserv.NodeNumbers.get(0).toString().equalsIgnoreCase(Nodeserv.Self_key_long.toString())){
			if(Nodeserv.NodeNumbers.size()==1){
				Nodeserv.Predeccessor=Nodeserv.NodeNumbers.get(0).toString();
			}
			else{
				Nodeserv.Predeccessor=Nodeserv.NodeNumbers.get(Nodeserv.NodeNumbers.size()-1).toString();
			}
		}
		else{
			for(Integer i = 1; i<=Nodeserv.NodeNumbers.size()-1; i++){
				if(Nodeserv.NodeNumbers.get(i).toString().equalsIgnoreCase(Nodeserv.Self_key_long.toString())){
					Nodeserv.Predeccessor=Nodeserv.NodeNumbers.get(i-1).toString();
				}

			}

		}
		if(Nodeserv.Self_key_long.toString().equalsIgnoreCase
				(Nodeserv.NodeNumbers.get(Nodeserv.NodeNumbers.size()-1).toString())){
			Nodeserv.SuccessorForSelfNode = Nodeserv.NodeNumbers.get(0).toString(); 
		}
		else{
			for(Integer i=0; i<Nodeserv.PeerDetails.size(); i++){
				if(Nodeserv.Self_key_long.toString().equalsIgnoreCase
						(Nodeserv.NodeNumbers.get(i).toString())){
					Nodeserv.SuccessorForSelfNode=Nodeserv.NodeNumbers.get(i+1).toString();
				}
			}
		}
		Nodeserv.FingerTable.clear();
		Nodeserv.createFingerTable(Nodeserv, Nodeserv.Self_key_long, Nodeserv.m);
	}

	//function to handle search request in self arrays
	public QueryResult SearchRequestInSelf(NodeServer NodeServ, String fileName) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		//get hash of filename
		String key =AuxFunctions.HashingFiles(fileName, NodeServ.m);
		QueryResult results = new QueryResult(" ", " ", false, (double) 0, 0, " ", " ");
		results.Found=false;
		for(int i =0; i< NodeServ.SelfFileTable.size(); i++){
			if((NodeServ.SelfFileTable.get(i).Filename).equalsIgnoreCase(fileName)){
				try{

					//search in self file table
					results.searchRequest = key;
					results.Found=true;
					results.FoundIP = NodeServ.curNode.nodeIP;
					results.FoundPort = NodeServ.curNode.nodePort.toString();
					results.HopCount = 0;
					results.OriginalFilename = NodeServ.SelfFileTable.get(i).OriginalString;
				}catch(NullPointerException e){
					e.printStackTrace();
				}
			}
		}
		if(results.Found.toString().equalsIgnoreCase("false")){
			for(int i=0; i<NodeServ.FilesAsSuccessor.size(); i++){
				if(fileName.equalsIgnoreCase(NodeServ.FilesAsSuccessor.get(i).Filename)){
					
					//search in self successor files
					try{
						results = new QueryResult(key, NodeServ.FilesAsSuccessor.get(i).OriginalString,true, NodeServ.StartQueryTime-NodeServ.EndQueryTime, 0, NodeServ.FilesAsSuccessor.get(i).IP, NodeServ.FilesAsSuccessor.get(i).Port);
						System.out.println(results.searchRequest + "\n" + results.Found + "\n" + results.FoundIP + "\n" + results.FoundPort + "\n" + results.HopCount + "\n" + results.OriginalFilename);
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}
			}
		}

		
		//if self should be successor and no result found, send empty result
		if(results.Found.toString().equalsIgnoreCase("false")){

			if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.Predeccessor) && 
					Integer.parseInt(key)>Integer.parseInt(NodeServ.Predeccessor) && Integer.parseInt(key)<= NodeServ.Self_key_long){
				try{
					results.Found = true;
					results.FoundIP = "-";
					results.FoundPort = "-";
					results.HopCount = 0;
					results.OriginalFilename = "-";
					results.searchRequest = key;
				}catch(NullPointerException e){
					e.printStackTrace();
				}
			}
			else if(NodeServ.Self_key_long<Integer.parseInt(NodeServ.Predeccessor) && Integer.parseInt(key)<= NodeServ.Self_key_long ){
				try{
					results.Found = true;
					results.FoundIP = "-";
					results.FoundPort = "-";
					results.HopCount = 0;
					results.OriginalFilename = "-";
					results.searchRequest = key;
				}
				catch(NullPointerException e){
					e.printStackTrace();
				}
			}
			if(NodeServ.Self_key_long < Integer.parseInt(NodeServ.Predeccessor) && 
					Integer.parseInt(key)<=NodeServ.Self_key_long){
				try{
					results.Found = true;
					results.FoundIP = "-";
					results.FoundPort = "-";
					results.HopCount = 0;
					results.OriginalFilename = "-";
					results.searchRequest = key;
				}catch(NullPointerException e){
					e.printStackTrace();
				}

			}
		}

		//send SER request to relevant peer

		if(results.Found.toString().equalsIgnoreCase("false")){
			try {
				String SER = " SER " + NodeServ.curNode.nodeIP + " " + NodeServ.curNode.nodePort + " " + key + " 1";
				SER = AuxFunctions.getMsg(SER);
				if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.SuccessorForSelfNode) && Integer.parseInt(key) > NodeServ.Self_key_long ){
					try{
						String IP_Port = CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						Socket sock = new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						PrintWriter writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(SER);
						System.out.println("Sent Msg: " + SER);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						results.Found =false;
						sock.close();
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}

				else if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.SuccessorForSelfNode) &&
						Integer.parseInt(key) <=Integer.parseInt(NodeServ.SuccessorForSelfNode)){
					try{
						String IP_Port = CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						Socket sock = new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						PrintWriter writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(SER);
						System.out.println("Sent Msg: " + SER);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						results.Found = false;
						sock.close();
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}


				else if(NodeServ.Self_key_long<Integer.parseInt(NodeServ.SuccessorForSelfNode) && Integer.parseInt(key)<=Integer.parseInt(NodeServ.SuccessorForSelfNode)
						&& Integer.parseInt(key) > NodeServ.Self_key_long){
					try{

						String IP_Port = CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						Socket sock = new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						PrintWriter writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(SER);
						System.out.println("Sent Msg: " + SER);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						sock.close();
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}

				else{
					try{

						String Succ = NodeServ.CheckSuccessorForFiles(NodeServ, Integer.parseInt(key));
						if(Integer.parseInt(Succ)==NodeServ.Self_key_long){
							results.Found = true;
							results.FoundIP = "-";
							results.FoundPort = "-";
							results.HopCount = 0;
							results.OriginalFilename = "-";
							results.searchRequest = key;

						}
						else{
							String IP_Port = CheckIPPortForSucc(NodeServ, Succ);
							String in[]=IP_Port.split(" ");
							Socket sock = new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
							PrintWriter writer = new PrintWriter(sock.getOutputStream(),true);
							writer.println(SER);
							System.out.println("Sent Msg: " + SER);
							System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
							sock.close();
							writer.close();
						}
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}
			} catch (NumberFormatException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return results;

	}

	//function to handle search request when SER is received
	
	public QueryResult SearchRequestWhenRequestReceived(NodeServer NodeServ, String key, String SearchingIP, String SearchingPort, Integer hopCount) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		QueryResult results = new QueryResult(null, null, false, (double) 0, 0, null, null);
		results.Found=false;
		for(int i =0; i< NodeServ.SelfFileTable.size(); i++){
			if(key.equalsIgnoreCase(NodeServ.SelfFileTable.get(i).KeyValue)){
				try{
					hopCount++;
					results = new QueryResult(key, 
							NodeServ.SelfFileTable.get(i).OriginalString, true, (double) 0, hopCount, NodeServ.curNode.nodeIP, NodeServ.curNode.nodePort.toString());
				}catch(NullPointerException e){
					e.printStackTrace();
				}
			}

		}
		if(results.Found.toString().equalsIgnoreCase("false")){
			for(int i=0; i<NodeServ.FilesAsSuccessor.size(); i++){
				if(NodeServ.FilesAsSuccessor.get(i).KeyValue.equalsIgnoreCase(key)){
					try{
						hopCount++;
						results = new QueryResult(key, 
								NodeServ.FilesAsSuccessor.get(i).OriginalString,true, (double) 0, hopCount, NodeServ.FilesAsSuccessor.get(i).IP, NodeServ.FilesAsSuccessor.get(i).Port);
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}
			}

		}

		if(results.Found.toString().equalsIgnoreCase("false")){

			if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.Predeccessor) && 
					Integer.parseInt(key)>Integer.parseInt(NodeServ.Predeccessor) && Integer.parseInt(key)<=NodeServ.Self_key_long){
				try{

					results.Found = true;
					results.FoundIP = "-";
					results.FoundPort = "-";
					results.HopCount = hopCount;
					results.OriginalFilename = "-";
					results.searchRequest = key;
				}catch(NullPointerException e){
					e.printStackTrace();
				}

			}
			else if(NodeServ.Self_key_long<Integer.parseInt(NodeServ.Predeccessor) && Integer.parseInt(key)<= NodeServ.Self_key_long ){
				try{

					results.Found = true;
					results.FoundIP = "-";
					results.FoundPort = "-";
					results.HopCount = hopCount;
					results.OriginalFilename = "-";
					results.searchRequest = key;
				}catch(NullPointerException e){
					e.printStackTrace();
				}	
			}
			if(NodeServ.Self_key_long < Integer.parseInt(NodeServ.Predeccessor) && 
					Integer.parseInt(key) > Integer.parseInt(NodeServ.Predeccessor)){
				try{

					results.Found = true;
					results.FoundIP = "-";
					results.FoundPort = "-";
					results.HopCount = hopCount;
					results.OriginalFilename = "-";
					results.searchRequest = key;
				}catch(NullPointerException e){
					e.printStackTrace();
				}

			}
		}

		if(results.Found.toString().equalsIgnoreCase("false")){
			try {
				hopCount++;
				String SER = " SER " + SearchingIP + " " + SearchingPort + " " + key + " " + hopCount;
				SER = AuxFunctions.getMsg(SER);
				if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.SuccessorForSelfNode) && Integer.parseInt(key) > NodeServ.Self_key_long ){
					try{

						String IP_Port = CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						Socket sock = new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						PrintWriter writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(SER);
						System.out.println("Sent Msg: " + SER);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						sock.close();
					}catch(NullPointerException e){
						e.printStackTrace();
					}


				}

				else if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.SuccessorForSelfNode) && 
						Integer.parseInt(key) <=Integer.parseInt(NodeServ.SuccessorForSelfNode)){
					try{

						String IP_Port = CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						Socket sock = new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						PrintWriter writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(SER);
						System.out.println("Sent Msg: " + SER);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						sock.close();
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}

				else if(NodeServ.Self_key_long<Integer.parseInt(NodeServ.SuccessorForSelfNode) && Integer.parseInt(key)<=Integer.parseInt(NodeServ.SuccessorForSelfNode)
						&& Integer.parseInt(key) > NodeServ.Self_key_long){
					try{

						String IP_Port = CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						Socket sock = new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						PrintWriter writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(SER);
						System.out.println("Sent Msg: " + SER);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						sock.close();
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}
				else{
					try{
						String succ = NodeServ.CheckSuccessorForFiles(NodeServ, Integer.parseInt(key));
						if(Integer.parseInt(succ)==NodeServ.Self_key_long){
							results.Found = true;
							results.FoundIP = "-";
							results.FoundPort = "-";
							results.HopCount = hopCount;
							results.OriginalFilename = "-";
							results.searchRequest = key;

						}
						else{
							String IP_Port = CheckIPPortForSucc(NodeServ, succ);
							String in[]=IP_Port.split(" ");
							Socket sock = new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
							PrintWriter writer = new PrintWriter(sock.getOutputStream(),true);
							writer.println(SER);
							System.out.println("Sent Msg: " + SER);
							System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
							sock.close();
							writer.close();
						}
					}catch(NullPointerException e){
						e.printStackTrace();
					}
				}
			} catch (NumberFormatException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		return results;

	}
	
	//function to create finger table
	public void createFingerTable(NodeServer nodeserv, Integer NodeNumber, Integer NoBits) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		ArrayList<String> Start = new ArrayList<String>();
		
		//generate start
		for(Integer i = 0; i< NoBits; i++){
			double a = Math.pow(2, (i))+NodeNumber;
			double b = Math.pow(2, NoBits);
			Integer start = (int) (a % b);
			Start.add(start.toString());
		}
		
		//generate interval
		ArrayList<String> Interval = new ArrayList<String>();
		for (Integer i=0; i<NoBits; i++){
			if(i== NoBits-1){
				Interval.add(Start.get(i) + " " + (Integer.parseInt(Start.get(0))-1) + " ");
			}
			else{
				Interval.add(Start.get(i) + " " + Start.get(i+1) + " ");
			}
		}
		ArrayList<String> Successor = new ArrayList<String>();
		//generate successor
		for(Integer i=0; i< NoBits; i++){
			String input[] = Interval.get(i).split(" ");
			if(Integer.parseInt(input[0].trim()) > nodeserv.NodeNumbers.get(nodeserv.NodeNumbers.size()-1) || 
					Integer.parseInt(input[0].trim()) < nodeserv.NodeNumbers.get(0)){
				Successor.add(nodeserv.NodeNumbers.get(0).toString());
			}
			else{
				for(Integer j = 0; j< nodeserv.NodeNumbers.size(); j++){
					if(Integer.parseInt(input[0].trim()) == nodeserv.NodeNumbers.get(j)){
						Successor.add(nodeserv.NodeNumbers.get(j).toString());
						break;
					}
				}
				for(Integer j = 0; j< nodeserv.NodeNumbers.size(); j++){
					if(Integer.parseInt(input[0].trim()) > nodeserv.NodeNumbers.get(j) 
							&& Integer.parseInt(input[0].trim()) < nodeserv.NodeNumbers.get(j+1)){
						Successor.add(nodeserv.NodeNumbers.get(j+1).toString());
						break;
					}

				}
			}
		}
		for(Integer i = 0; i<NoBits; i++){
			nodeserv.FingerTable.add(new FingerTable(Start.get(i), Interval.get(i), Successor.get(i)));
		}
	}

	//function to check IP and Port of a peer
	public String CheckIPPortForSucc(NodeServer NodeServ, String succ){

		String IP_Port = null;
		for(Integer i =0; i<NodeServ.PeerDetails.size(); i++){
			if(succ.equalsIgnoreCase(NodeServ.PeerDetails.get(i).nodeNumberKeyInLong.toString())){
				IP_Port = NodeServ.PeerDetails.get(i).nodeIP + " " + NodeServ.PeerDetails.get(i).nodePort;

			}

		}
		return IP_Port;
	}

	//function to check Successor of a key in fingertable
	public String CheckSuccessorForFiles(NodeServer Nodeserv, Integer KeyValue){
		Integer DefaultValue = 0;
		String Successor = "";
		for(Integer i =0; i<Nodeserv.FingerTable.size(); i++){
			String input[] = Nodeserv.FingerTable.get(i).interval.split(" ");
			Integer initialValue = Integer.parseInt(input[0].trim());
			Integer finalValue = Integer.parseInt(input[1].trim());
			if (finalValue < initialValue){
				DefaultValue = Integer.parseInt(Nodeserv.FingerTable.get(i).successor);
			}
		}
		outerloop:
			for(Integer i =0; i<Nodeserv.FingerTable.size(); i++){
				String input[] = Nodeserv.FingerTable.get(i).interval.split(" ");
				Integer initialValue = Integer.parseInt(input[0].trim());
				Integer finalValue = Integer.parseInt(input[1].trim());
				if(KeyValue >= initialValue && KeyValue < finalValue){
					Successor = new String(Nodeserv.FingerTable.get(i).successor);
					break outerloop;
				}

				else{
					Successor = new String(DefaultValue.toString());
				}
			}
		return Successor;
	}

	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
		NodeServer.Self_Port = Integer.parseInt(args[2]);
		NodeServer NodeServ = new NodeServer();


		if (args.length > 0) {
			try {
				NodeServ.BootstrapIP = args[0];
				NodeServ.BootstrapPort = Integer.parseInt(args[1]);
			} catch (NumberFormatException ex) {
				System.out.println("The argument passed is not of the correct Data-type.");
			}
		}
		NodeServ.NodeNumbers.add(NodeServ.Self_key_long);

		//Connect to the Bootstrap Server
		NodeServ.connect_to_bootstrap(NodeServ);
		NodeServ.NodeNumbers = AuxFunctions.Sortarray(NodeServ.NodeNumbers);
		if(NodeServ.NodeNumbers.get(0).toString().equalsIgnoreCase(NodeServ.Self_key_long.toString())){
			if(NodeServ.NodeNumbers.size()==1){
				NodeServ.Predeccessor=NodeServ.NodeNumbers.get(0).toString();
			}
			else{
				NodeServ.Predeccessor=NodeServ.NodeNumbers.get(NodeServ.NodeNumbers.size()-1).toString();
			}
		}
		else{
			for(Integer i = 1; i<=NodeServ.NodeNumbers.size()-1; i++){
				if(NodeServ.NodeNumbers.get(i).toString().equalsIgnoreCase(NodeServ.Self_key_long.toString())){
					NodeServ.Predeccessor=NodeServ.NodeNumbers.get(i-1).toString();
				}

			}

		}



		//create server socket
		@SuppressWarnings("resource")
		ServerSocket server = new ServerSocket(NodeServ.curNode.nodePort);
		String UpdateFinger = " UPFIN 0 " + NodeServ.curNode.nodeIP + " " + NodeServ.curNode.nodePort + 
				" " + NodeServ.curNode.nodeNumberKey;
		UpdateFinger = AuxFunctions.getMsg(UpdateFinger);
		//send UPFIN request to peers
		Socket ClientSocket = new Socket();
		for (Integer i = 0; i< NodeServ.PeerDetails.size(); i++){
			InetAddress CurrentPeerIP = InetAddress.getByName(NodeServ.PeerDetails.get(i).nodeIP);
			Integer CurrentPeerPort = NodeServ.PeerDetails.get(i).nodePort;
			ClientSocket = new Socket(CurrentPeerIP, CurrentPeerPort);
			PrintWriter writer = new PrintWriter(ClientSocket.getOutputStream(),true);
			writer.println(UpdateFinger);
		}
		ClientSocket.close();
		
		//start thread for system inputs
		ClientSocket =new Socket();
		Runnable runn = new ConsoleReader(NodeServ);
		Thread t = new Thread(runn);
		t.start();
		NodeServer.upfinokcounter=0;
		NodeServer.numOfAddSent = 0;
		NodeServer.numOfADDOKrecd = 0;
		while (true){
			
			//program on continous listen
			ClientSocket = server.accept();
			InputStreamReader in =  new InputStreamReader(ClientSocket.getInputStream());
			BufferedReader reader = new BufferedReader(in);
			String read = reader.readLine();
			String input[] = read.split(" ");
			String recd_msg = input[1].trim();
			
			//start thread for socket accept 
			Runnable runn_1 = new MessageHandler(ClientSocket, NodeServ, recd_msg, input, read);
			numberOfMsgs++;
			System.out.println("Number of Msgs handled by Node : " + numberOfMsgs++);
			Thread t1 = new Thread(runn_1);
			//Start the thread and go to run()
			t1.start();
		}
	}
}
