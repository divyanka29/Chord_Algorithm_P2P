import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ConsoleReader implements Runnable {

	private NodeServer NodeServ = null;
	public ConsoleReader(NodeServer NodeServ) {
		this.NodeServ = NodeServ;
	}

	public void run() {
		BufferedReader System_in = new BufferedReader(new InputStreamReader(System.in));

		String system_input= "";
		Boolean flag = true;
		try {
			while ((system_input.equalsIgnoreCase("quit") == false) && (flag)) {
				system_input = System_in.readLine();
				System.out.println();
				
				//function to display fingertable
				if(system_input.equalsIgnoreCase("fingertable")){

					System.out.println("Finger Table");
					System.out.format("%-10s %20s %10s\n", "Start", "Interval", "Successor");
					for(Integer i = 0; i< NodeServ.m; i++){
						System.out.format("%-10s %20s %10s\n", NodeServ.FingerTable.get(i).start,
								NodeServ.FingerTable.get(i).interval, NodeServ.FingerTable.get(i).successor);
					}
					System.out.println();
				}
				
				else if(system_input.equalsIgnoreCase("nodenumbers")){
					for(int i =0; i<NodeServ.NodeNumbers.size(); i++){
						System.out.println(NodeServ.NodeNumbers.get(i));
					}

				}
				
				//function to display keytable
				else if(system_input.equalsIgnoreCase("keytable")){
					System.out.format("%-15s %5s %10s %10s\n", "IP", "Port", "Key in Hex", "Key in Integer");
					for(Integer i = 0; i< NodeServ.PeerDetails.size(); i++){
						System.out.format("%-15s %5s %10s %10s\n", NodeServ.PeerDetails.get(i).nodeIP, 
								NodeServ.PeerDetails.get(i).nodePort, NodeServ.PeerDetails.get(i).nodeNumberKey,
								NodeServ.PeerDetails.get(i).nodeNumberKeyInLong);
					}
					System.out.println();
				}
				
				//function to display self files
				else if(system_input.equalsIgnoreCase("files") || system_input.equalsIgnoreCase("2")){
					System.out.println("Length of Self File Table : " + NodeServ.SelfFileTable.size());
					System.out.format("%-20s %10s %20s\n", "Filename", "Key value", "Original String");
					for(Integer i = 0; i<NodeServ.SelfFileTable.size(); i++){
						System.out.format("%-20s %10s %20s\n", NodeServ.SelfFileTable.get(i).Filename, 
								NodeServ.SelfFileTable.get(i).KeyValue, NodeServ.SelfFileTable.get(i).OriginalString);
					}

				}
				
				//funtion to display self IP, Port, Key
				else if(system_input.equalsIgnoreCase("self") || system_input.equalsIgnoreCase("1")){
					System.out.println("Node Details : ");
					System.out.println("IP : " + NodeServ.curNode.nodeIP);
					System.out.println("Port number : " + NodeServ.curNode.nodePort); 
					System.out.println("Key in Hex : " + NodeServ.curNode.nodeNumberKey);
					System.out.println("Key in Integer : " + NodeServ.curNode.nodeNumberKeyInLong);
				}
				
				else if(system_input.equalsIgnoreCase("predecessor")){
					System.out.println(NodeServ.Predeccessor);

				}
				else if(system_input.equalsIgnoreCase("successor")){
					System.out.println(NodeServ.SuccessorForSelfNode);

				}
				
				//function to display files for which current node is successor
				else if(system_input.equalsIgnoreCase("filesassuccessor") || system_input.equalsIgnoreCase("3")){
					System.out.println("Length of File As Successor Table : " + NodeServ.FilesAsSuccessor.size());
					System.out.format("%-20s %10s %20s %15s %5s\n", "Filename", "KeyValue", "OriginalString", "IP", "Port");
					for(Integer i = 0; i <NodeServ.FilesAsSuccessor.size(); i++){

						System.out.format("%-20s %10s %20s %15s %5s\n", NodeServ.FilesAsSuccessor.get(i).Filename, 
								NodeServ.FilesAsSuccessor.get(i).KeyValue, NodeServ.FilesAsSuccessor.get(i).OriginalString
								, NodeServ.FilesAsSuccessor.get(i).IP, NodeServ.FilesAsSuccessor.get(i).Port);
					}

				}
				
				//function to leave 
				else if(system_input.equalsIgnoreCase("leave")){
					NodeServ.UnregWithBootstrap(NodeServ);
					


				}
				
				//function to start search request
				else if(system_input.equalsIgnoreCase("search")){
					NodeServ.queryresult.clear();
					NodeServ.queryList.clear();
					NodeServ.HashOfQueryList.clear();
					NodeServ.resultFile = NodeServ.curNode.nodeIP + "_"+NodeServ.curNode.nodePort + ".txt";
					File result = new File(NodeServ.resultFile);
					if(result.exists())
						result.delete();
					result.createNewFile();
					BufferedReader br;
					//read from queries.txt
					br = new BufferedReader(new FileReader("queries.txt"));
					String line;
					
					//generate hash of filenames
					while ((line = br.readLine()) != null) {
						String line1 = line.toLowerCase();
						String line2 = line1.replaceAll(" ", "_").trim();
						NodeServ.queryList.add(line2);
						NodeServ.HashOfQueryList.add(AuxFunctions.HashingFiles(line2, NodeServ.m));
					}
					br.close();
					ArrayList<QueryResult> temp = new ArrayList<QueryResult>();
//start search
					for(int i=0; i<NodeServ.queryList.size(); i++){
						NodeServ.StartQueryTime = Double.parseDouble(String.format("%.3f", System.currentTimeMillis() / 1000.0));
						temp.add(NodeServ.SearchRequestInSelf(NodeServ, NodeServ.queryList.get(i)));
						if(temp.get(i).Found.toString().equalsIgnoreCase("true")){
							NodeServ.EndQueryTime = Double.parseDouble(String.format("%.3f", System.currentTimeMillis() / 1000.0));
							temp.get(i).QueryTime = NodeServ.EndQueryTime-NodeServ.StartQueryTime;
						}
						Thread.sleep(1000);
					}

					for(int i=0; i<temp.size(); i++){
						if(temp.get(i).Found.toString().equalsIgnoreCase("true")){
							NodeServ.queryresult.add(temp.get(i));
						}
					}

					for(int i=0; i< NodeServ.queryresult.size(); i++){
						for(int j=0; j<NodeServ.queryList.size(); j++){
							if(NodeServ.queryresult.get(i).searchRequest.equalsIgnoreCase(NodeServ.HashOfQueryList.get(j))){
								NodeServ.queryresult.get(i).searchRequest=NodeServ.queryList.get(j);
							}
						}
					}
					NodeServ.queryresult.trimToSize();
					FileWriter fw = new FileWriter(result.getAbsoluteFile());
					BufferedWriter bw = new BufferedWriter(fw);	
					for(int i = 0; i<NodeServ.queryresult.size(); i++){
						try{
							String str =null;
							if(NodeServ.queryresult.get(i).searchRequest.contains(" ")){
								str = NodeServ.queryresult.get(i).searchRequest.replace("_", " ");
							}
							else{
								str = NodeServ.queryresult.get(i).searchRequest;
							}

							bw.write(str + " " + NodeServ.queryresult.get(i).OriginalFilename + " " + NodeServ.queryresult.get(i).FoundIP
									+ " " + NodeServ.queryresult.get(i).FoundPort + " " + NodeServ.queryresult.get(i).QueryTime + " " + NodeServ.queryresult.get(i).HopCount
									+ "\n");

						}catch(NullPointerException e)
						{
							e.printStackTrace();
						}
					}
					bw.close();
				}
			}
			System_in.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}