import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;


public class MessageHandler implements Runnable{

	private NodeServer NodeServ = null;
	private Socket sock;
	private String recd_msg = "";
	private String input[];
	private String read = "";
	public MessageHandler(Socket sock, NodeServer NodeServ, String recd_msg, String input[], String read) {
		this.NodeServ = NodeServ;
		this.sock = sock;
		this.input = input;
		this.read = read;
		this.recd_msg = recd_msg;
	}

	public void run() {


		if(recd_msg.equalsIgnoreCase("UPFIN")){
			System.out.println("\nRcvd Msg: " + read);
			System.out.println("From : " + sock.getInetAddress() + " " + input[4].trim());
			String recd_type = input[2].trim();
			String RecvPeerIP = input[3].trim();
			String RecvPeerPort = input[4].trim();
			String RecvPeerKey = input[5].trim();
			if(recd_type.equalsIgnoreCase("0")){

				/*New Entry in the network*/

				NodeServ.UpdatePeerDetailsEnter(NodeServ, RecvPeerIP, RecvPeerPort, RecvPeerKey);
				ArrayList<FilesKeyTable> temp = new ArrayList<FilesKeyTable>();
				for(int i =0; i<NodeServ.SelfFileTable.size(); i++){
					temp.add(new FilesKeyTable(NodeServ.SelfFileTable.get(i).Filename, NodeServ.SelfFileTable.get(i).KeyValue, 
							NodeServ.SelfFileTable.get(i).OriginalString, NodeServ.SelfFileTable.get(i).IP, 
							NodeServ.SelfFileTable.get(i).Port));
				}
				NodeServ.SelfFileTable.clear();
				for(int i=0; i<temp.size(); i++){
					NodeServ.SelfFileTable.add(temp.get(i));
				}
				temp.clear();
				try {
					sock = new Socket(InetAddress.getByName(RecvPeerIP), Integer.parseInt(RecvPeerPort));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//send UPFINOK
				String upfinok = " UPFINOK 0 " + NodeServ.Self_key_long;
				String upfinok1 = AuxFunctions.getMsg(upfinok);
				PrintWriter writer;
				try {
					writer = new PrintWriter(sock.getOutputStream(),true);
					writer.println(upfinok1);

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}


			else{
				/*peer Leaving the network*/
				try {
					NodeServ.UpdatePeerDetailsLeave(NodeServ, RecvPeerIP, RecvPeerPort, RecvPeerKey);
				} catch (UnsupportedEncodingException
						| NoSuchAlgorithmException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				try {
					sock = new Socket(InetAddress.getByName(RecvPeerIP), Integer.parseInt(RecvPeerPort));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String upfinok = " UPFINOKLeave 0 " + NodeServ.Self_key_long;
				String upfinok1 = AuxFunctions.getMsg(upfinok);
				PrintWriter writer;
				try {
					writer = new PrintWriter(sock.getOutputStream(),true);
					writer.println(upfinok1);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				sock.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(recd_msg.equalsIgnoreCase("UPFINOK")){
			
			Boolean ToSendWithAdd;
			NodeServer.upfinokcounter++;
			System.out.println("\nRcvd Msg: " + read);
			System.out.println("From : " + sock.getInetAddress() + " " + sock.getPort());
			NodeServ.FilesAsSuccessor.clear();
			try {
				Thread.sleep(4000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for(int i =0; i<NodeServ.SelfFileTable.size(); i++){
				ToSendWithAdd =false;

				String succ = NodeServ.CheckSuccessorForFiles(NodeServ, Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue));
				if(NodeServ.Self_key_long > Integer.parseInt(NodeServ.Predeccessor) &&
						Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue) <= NodeServ.Self_key_long && 
						Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue)>Integer.parseInt(NodeServ.Predeccessor)){
					NodeServ.FilesAsSuccessor.add(NodeServ.SelfFileTable.get(i));
					ToSendWithAdd = true;
				}

				else if(NodeServ.Self_key_long < Integer.parseInt(NodeServ.Predeccessor)
						&& Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue) > Integer.parseInt(NodeServ.Predeccessor)){
					NodeServ.FilesAsSuccessor.add(NodeServ.SelfFileTable.get(i));
					ToSendWithAdd = true;
				}
				else if(NodeServ.Self_key_long < Integer.parseInt(NodeServ.Predeccessor) &&
						Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue)<=NodeServ.Self_key_long){
					NodeServ.FilesAsSuccessor.add(NodeServ.SelfFileTable.get(i));
					ToSendWithAdd = true;
				}
				else if(succ.equalsIgnoreCase(NodeServ.Self_key_long.toString())){
					NodeServ.FilesAsSuccessor.add(NodeServ.SelfFileTable.get(i));
					ToSendWithAdd = true;
				}
				if(ToSendWithAdd.toString().equalsIgnoreCase("false")){

					if(NodeServ.Self_key_long<Integer.parseInt(NodeServ.SuccessorForSelfNode) &&
							Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue) > NodeServ.Self_key_long && 
							Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue) < Integer.parseInt(NodeServ.SuccessorForSelfNode)){
						String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						try {
							sock =new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						} catch (NumberFormatException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						String AddKey_1 = (" ADD " + NodeServ.curNode.nodeIP + " " + NodeServ.curNode.nodePort + " " + NodeServ.SelfFileTable.get(i).KeyValue
								+ " " + NodeServ.SelfFileTable.get(i).Filename + " " + NodeServ.SelfFileTable.get(i).OriginalString);
						String Addkey_1= AuxFunctions.getMsg(AddKey_1);
						PrintWriter writer2;
						try {
							writer2 = new PrintWriter(sock.getOutputStream(),true);
							writer2.println(Addkey_1);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						NodeServer.numOfAddSent++;
						System.out.println("Sent Msg: " + Addkey_1);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						try {
							sock.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						ToSendWithAdd = true;

					}

					else if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.SuccessorForSelfNode) && 
							Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue)>NodeServ.Self_key_long){
						String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						try {
							sock =new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						} catch (NumberFormatException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						String AddKey_1 = (" ADD " + NodeServ.curNode.nodeIP + " " + NodeServ.curNode.nodePort + " " + NodeServ.SelfFileTable.get(i).KeyValue
								+ " " + NodeServ.SelfFileTable.get(i).Filename + " " + NodeServ.SelfFileTable.get(i).OriginalString);
						String Addkey_1= AuxFunctions.getMsg(AddKey_1);
						PrintWriter writer2;
						try {
							writer2 = new PrintWriter(sock.getOutputStream(),true);
							writer2.println(Addkey_1);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						NodeServer.numOfAddSent++;
						System.out.println("Sent Msg: " + Addkey_1);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						try {
							sock.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						ToSendWithAdd = true;
					}
					else if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.SuccessorForSelfNode) && 
							Integer.parseInt(NodeServ.SelfFileTable.get(i).KeyValue) < Integer.parseInt(NodeServ.SuccessorForSelfNode)){

						String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
						String in[]=IP_Port.split(" ");
						try {
							sock =new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
						} catch (NumberFormatException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						String AddKey_1 = (" ADD " + NodeServ.curNode.nodeIP + " " + NodeServ.curNode.nodePort + " " + NodeServ.SelfFileTable.get(i).KeyValue
								+ " " + NodeServ.SelfFileTable.get(i).Filename + " " + NodeServ.SelfFileTable.get(i).OriginalString);
						String Addkey_1= AuxFunctions.getMsg(AddKey_1);
						PrintWriter writer2;
						try {
							writer2 = new PrintWriter(sock.getOutputStream(),true);
							writer2.println(Addkey_1);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						NodeServer.numOfAddSent++;
						System.out.println("Sent Msg: " + Addkey_1);
						System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
						try {
							sock.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						ToSendWithAdd = true;
					}
				}
				if(ToSendWithAdd.toString().equalsIgnoreCase("false")){
					String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, succ);
					String in[]=IP_Port.split(" ");
					try {
						sock =new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					String AddKey_1 = (" ADD " + NodeServ.curNode.nodeIP + " " + NodeServ.curNode.nodePort + " " + NodeServ.SelfFileTable.get(i).KeyValue
							+ " " + NodeServ.SelfFileTable.get(i).Filename + " " + NodeServ.SelfFileTable.get(i).OriginalString);
					String Addkey_1= AuxFunctions.getMsg(AddKey_1);
					PrintWriter writer2;
					try {
						writer2 = new PrintWriter(sock.getOutputStream(),true);
						writer2.println(Addkey_1);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					NodeServer.numOfAddSent++;
					System.out.println("Sent Msg: " + Addkey_1);
					System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
					try {
						sock.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}
			try {
				Thread.sleep(6000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
			String in[]=IP_Port.split(" ");
			try {
				sock =new Socket(InetAddress.getByName(in[0].trim()), Integer.parseInt(in[1].trim()));
			} catch (NumberFormatException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String GetKey = " GETKY " + NodeServ.Self_key_long;
			String Getkey1 = AuxFunctions.getMsg(GetKey);
			PrintWriter writer;
			System.out.println();
			System.out.println("Sent Msg: " + Getkey1);
			System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
			try {
				writer = new PrintWriter(sock.getOutputStream(),true);
				writer.println(Getkey1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				sock.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//			}
		}

		else if(recd_msg.equalsIgnoreCase("UPFINOKLeave")){
			NodeServer.upfinleave++;
			if(NodeServer.upfinleave == NodeServ.PeerDetails.size()){
				for(int i =0; i< NodeServ.NodeNumbers.size(); i++){
					if(NodeServ.Self_key_long.intValue()==NodeServ.NodeNumbers.get(i).intValue()){
						NodeServ.NodeNumbers.remove(i);
					}
				}
			}
		}

		else if(recd_msg.equalsIgnoreCase("ADD")){
			System.out.println("\nRcvd Msg: " + read);
			System.out.println("From : " + sock.getInetAddress() + " " + sock.getPort());
			Integer FileKey = Integer.parseInt(input[4].trim());
			String Addok = " ADDOK 0";
			String addok1 = AuxFunctions.getMsg(Addok);
			NodeServ.ADDFlag =false;

			if(NodeServ.Self_key_long > Integer.parseInt(NodeServ.Predeccessor) &&
					FileKey <= NodeServ.Self_key_long && FileKey>Integer.parseInt(NodeServ.Predeccessor)){
				NodeServ.FilesAsSuccessor.add(new FilesKeyTable(input[5].trim(), input[4].trim(), 
						input[6].trim(), input[2].trim(), input[3].trim()));
				try {
					sock = new Socket(InetAddress.getByName(input[2].trim()), Integer.parseInt(input[3].trim()));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				PrintWriter writer;
				try {
					writer = new PrintWriter(sock.getOutputStream(),true);
					writer.println(addok1);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println("Sent Msg: " + addok1);
				System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
				NodeServ.ADDFlag=true;
				try {
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else if(Integer.parseInt(NodeServ.Predeccessor)>NodeServ.Self_key_long && FileKey <= NodeServ.Self_key_long){
				NodeServ.FilesAsSuccessor.add(new FilesKeyTable(input[5].trim(), input[4].trim(), 
						input[6].trim(), input[2].trim(), input[3].trim()));
				try {
					sock = new Socket(InetAddress.getByName(input[2].trim()), Integer.parseInt(input[3].trim()));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				PrintWriter writer;
				try {
					writer = new PrintWriter(sock.getOutputStream(),true);
					writer.println(addok1);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println("Sent Msg: " + addok1);
				System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
				NodeServ.ADDFlag=true;
				try {
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else if(Integer.parseInt(NodeServ.Predeccessor)>NodeServ.Self_key_long && FileKey > Integer.parseInt(NodeServ.Predeccessor)){
				NodeServ.FilesAsSuccessor.add(new FilesKeyTable(input[5].trim(), input[4].trim(), 
						input[6].trim(), input[2].trim(), input[3].trim()));
				try {
					sock = new Socket(InetAddress.getByName(input[2].trim()), Integer.parseInt(input[3].trim()));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				PrintWriter writer;
				try {
					writer = new PrintWriter(sock.getOutputStream(),true);
					writer.println(addok1);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Sent Msg: " + addok1);
				System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
				NodeServ.ADDFlag=true;
				try {
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if(NodeServ.ADDFlag.toString().equalsIgnoreCase("false")){

				if(NodeServ.Self_key_long < Integer.parseInt(NodeServ.SuccessorForSelfNode) && FileKey <=Integer.parseInt(NodeServ.SuccessorForSelfNode)
						&& FileKey > NodeServ.Self_key_long){
					String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
					String Peer[] = IP_Port.split(" ");
					try {
						sock = new Socket(InetAddress.getByName(Peer[0].trim()), Integer.parseInt(Peer[1].trim()));
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					PrintWriter writer;
					try {
						writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(read);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					System.out.println("Sent Msg: " + read);
					System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
					NodeServ.ADDFlag=true;
					try {
						sock.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				else if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.SuccessorForSelfNode) && FileKey > NodeServ.Self_key_long){
					String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
					String Peer[] = IP_Port.split(" ");
					try {
						sock = new Socket(InetAddress.getByName(Peer[0].trim()), Integer.parseInt(Peer[1].trim()));
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					PrintWriter writer;
					try {
						writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(read);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					System.out.println("Sent Msg: " + read);
					System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
					NodeServ.ADDFlag=true;
					try {
						sock.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				else if(NodeServ.Self_key_long>Integer.parseInt(NodeServ.SuccessorForSelfNode) && FileKey <= Integer.parseInt(NodeServ.SuccessorForSelfNode)){
					String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, NodeServ.SuccessorForSelfNode);
					String Peer[] = IP_Port.split(" ");
					try {
						sock = new Socket(InetAddress.getByName(Peer[0].trim()), Integer.parseInt(Peer[1].trim()));
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					PrintWriter writer;
					try {
						writer = new PrintWriter(sock.getOutputStream(),true);
						writer.println(read);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					System.out.println("Sent Msg: " + read);
					System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
					NodeServ.ADDFlag=true;
					try {
						sock.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}

			if(NodeServ.ADDFlag.toString().equalsIgnoreCase("false")){
				String succ = NodeServ.CheckSuccessorForFiles(NodeServ, FileKey);
				String IP_Port = NodeServ.CheckIPPortForSucc(NodeServ, succ);
				String Peer[] = IP_Port.split(" ");
				try {
					sock = new Socket(InetAddress.getByName(Peer[0].trim()), Integer.parseInt(Peer[1].trim()));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				PrintWriter writer;
				try {
					writer = new PrintWriter(sock.getOutputStream(),true);
					writer.println(read);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println("Sent Msg: " + read);
				System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
				try {
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}



		else if(recd_msg.equalsIgnoreCase("ADDOK")){
			NodeServer.numOfADDOKrecd++;
			System.out.println("\nRcvd Msg: " + read);
			System.out.println("From : " + sock.getInetAddress() + " " + sock.getPort());

		} 
		else if(recd_msg.equalsIgnoreCase("GETKY")){
			
			NodeServ.numOfGetkey++;
		
			if(NodeServ.numOfGetkey==1){
				String GetKeyFiles = null;

				System.out.println("\nRcvd Msg: " + read);
				System.out.println("From : " + sock.getInetAddress() + " " + sock.getPort());

				String RecvIP = sock.getInetAddress().getHostAddress();
				try {
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Integer RecvPort = 0;
				for(int i=0; i<NodeServ.PeerDetails.size(); i++){
					if(RecvIP.equalsIgnoreCase(NodeServ.PeerDetails.get(i).nodeIP)){
						RecvPort=NodeServ.PeerDetails.get(i).nodePort;
					}
				}
				ArrayList<FilesKeyTable> files = new ArrayList<FilesKeyTable>();
				String Keyvalue = input[2].trim();
				if(Integer.parseInt(Keyvalue)<NodeServ.Self_key_long){
					for(int i = 0; i< NodeServ.FilesAsSuccessor.size(); i++){
						if(Integer.parseInt(Keyvalue)>=Integer.parseInt(NodeServ.FilesAsSuccessor.get(i).KeyValue)){
							files.add(new FilesKeyTable(NodeServ.FilesAsSuccessor.get(i).Filename, NodeServ.FilesAsSuccessor.get(i).KeyValue,
									NodeServ.FilesAsSuccessor.get(i).OriginalString, NodeServ.FilesAsSuccessor.get(i).IP, NodeServ.FilesAsSuccessor.get(i).Port));
						}
						else if(Integer.parseInt(NodeServ.FilesAsSuccessor.get(i).KeyValue)>
						NodeServ.NodeNumbers.get(NodeServ.NodeNumbers.size()-1)){
							files.add(new FilesKeyTable(NodeServ.FilesAsSuccessor.get(i).Filename, NodeServ.FilesAsSuccessor.get(i).KeyValue,
									NodeServ.FilesAsSuccessor.get(i).OriginalString, NodeServ.FilesAsSuccessor.get(i).IP, NodeServ.FilesAsSuccessor.get(i).Port));						}
					}
				}
				else if(Integer.parseInt(Keyvalue) > NodeServ.Self_key_long){
					for(Integer i=0; i<NodeServ.FilesAsSuccessor.size(); i++){
						if(Integer.parseInt(NodeServ.FilesAsSuccessor.get(i).KeyValue) > NodeServ.Self_key_long && 
								Integer.parseInt(NodeServ.FilesAsSuccessor.get(i).KeyValue) <= Integer.parseInt(Keyvalue)){
							files.add(new FilesKeyTable(NodeServ.FilesAsSuccessor.get(i).Filename, NodeServ.FilesAsSuccessor.get(i).KeyValue,
									NodeServ.FilesAsSuccessor.get(i).OriginalString, NodeServ.FilesAsSuccessor.get(i).IP, NodeServ.FilesAsSuccessor.get(i).Port));						}
					}
				}





				StringBuilder getFiles = new StringBuilder(100);
				for(int i =0; i < files.size(); i++){
					getFiles.append(files.get(i).IP + " " + files.get(i).Port + " " + 
							files.get(i).KeyValue + " " + files.get(i).Filename + " " + files.get(i).OriginalString + " ");

				}
				GetKeyFiles = getFiles.toString();
				try {
					sock = new Socket(InetAddress.getByName(RecvIP), RecvPort);
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String Getkeyok = " GETKYOK " + files.size() + " " + GetKeyFiles;
				String GetKeyOk1 = AuxFunctions.getMsg(Getkeyok);
				PrintWriter writer;
				try {
					writer = new PrintWriter(sock.getOutputStream(),true);
					writer.println(GetKeyOk1);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Sent Msg: " + GetKeyOk1);
				System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
				try {
					Thread.sleep(1000*files.size());
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				NodeServ.FilesAsSuccessor.trimToSize();
				for(int i=0; i<NodeServ.FilesAsSuccessor.size(); i++){
					for(int j=0; j<files.size(); j++){

						if(files.get(j).KeyValue.equalsIgnoreCase(NodeServ.FilesAsSuccessor.get(i).KeyValue)){
							NodeServ.FilesAsSuccessor.remove(i);
							NodeServ.FilesAsSuccessor.trimToSize();
						}
					}
				}



				try {
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else{
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			NodeServ.numOfGetkey=0;
		}
		else if(recd_msg.equalsIgnoreCase("GETKYOK")){
			System.out.println("\nRcvd Msg: " + read);
			System.out.println("From : " + sock.getInetAddress() + " " + sock.getPort());
			Integer NumOfKeys = Integer.parseInt(input[2].trim());
			Integer i =3; 
			while(NumOfKeys.intValue()!=0){

				NodeServ.FilesAsSuccessor.add(new FilesKeyTable(input[i+3].trim(), input[i+2].trim(),
						input[i+4].trim(), input[i].trim(), input[i+1].trim()));
				NumOfKeys--;
				i = i+5;

			}
			try {
				sock.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else if(recd_msg.equalsIgnoreCase("SER")){
			System.out.println("\nRcvd Msg: " + read);
			System.out.println("From : " + sock.getInetAddress() + " " + sock.getPort());
			String SendSearchResultIP = input[2].trim();
			String SendSearchResultPort = input[3].trim();
			Integer hopCount = Integer.parseInt(input[5].trim());
			QueryResult queryresult = null;
			try {
				queryresult = NodeServ.SearchRequestWhenRequestReceived(NodeServ, input[4].trim(), SendSearchResultIP, SendSearchResultPort, hopCount);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if(queryresult.Found.booleanValue()==true){

				String SEROK = " SEROK 1 " + queryresult.FoundIP + " " + queryresult.FoundPort + " " + queryresult.OriginalFilename + " " + 
				queryresult.searchRequest + " " + queryresult.HopCount;
				SEROK = AuxFunctions.getMsg(SEROK);
				try {
					sock=new Socket(InetAddress.getByName(SendSearchResultIP), Integer.parseInt(SendSearchResultPort));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				PrintWriter writer;
				try {
					writer = new PrintWriter(sock.getOutputStream(),true);
					writer.println(SEROK);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println("Sent Msg: " + SEROK);
				System.out.println("To: " + sock.getInetAddress() + " " + sock.getPort());
				try {
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

		else if(recd_msg.equalsIgnoreCase("SEROK")){
			System.out.println("\nRcvd Msg: " + read);
			System.out.println("From : " + sock.getInetAddress() + " " + sock.getPort());
			NodeServ.EndQueryTime = Double.parseDouble(String.format("%.3f", System.currentTimeMillis() / 1000.0));
			Integer hopcountinmsghandler =0;
			hopcountinmsghandler =Integer.parseInt(input[7].trim());
			String Filename=null;
			for(int i=0; i<NodeServ.queryList.size(); i++){
				if(input[6].trim().equalsIgnoreCase(NodeServ.HashOfQueryList.get(i))){
					Filename = NodeServ.queryList.get(i);
					break;
				}
			}

			try {
				NodeServ.queryresult.add(new QueryResult(Filename, input[5].trim(), true, NodeServ.EndQueryTime-NodeServ.StartQueryTime, hopcountinmsghandler, input[3].trim(),
						input[4].trim()));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			NodeServ.resultFile = NodeServ.curNode.nodeIP+"_"+NodeServ.curNode.nodePort + ".txt";
			File result = new File(NodeServ.resultFile);
			if(result.exists())
				result.delete();
			try {
				result.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			FileWriter fw = null;
			try {
				fw = new FileWriter(result.getAbsoluteFile());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			BufferedWriter bw = new BufferedWriter(fw);	
			for(int i = 0; i<NodeServ.queryresult.size(); i++){
				try {
					String str =null;
					str = NodeServ.queryresult.get(i).searchRequest.replace("_", " ");
					bw.write(str + " " + NodeServ.queryresult.get(i).OriginalFilename + " " + NodeServ.queryresult.get(i).FoundIP
							+ " " + NodeServ.queryresult.get(i).FoundPort + " " + NodeServ.queryresult.get(i).QueryTime +  " " + NodeServ.queryresult.get(i).HopCount + "\n");

				}catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();

				}
			}
			try {
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(recd_msg.equalsIgnoreCase("GIVEKEY")){
			String RecvPeer = sock.getInetAddress().toString();
			Integer RecvPort = sock.getPort();
			for(int i=0; i< Integer.parseInt(input[2].trim()); i++){
				NodeServ.FilesAsSuccessor.add(new FilesKeyTable(input[3].trim(), 
						input[5].trim(), input[4].trim(), input[6].trim(), input[7].trim()));
			}
			try {
				sock.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				sock = new Socket(InetAddress.getByName(RecvPeer), RecvPort);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String givekeyok = " GIVEKEYOK 0 ";
			givekeyok = AuxFunctions.getMsg(givekeyok);
		}

		else if(recd_msg.equalsIgnoreCase("GIVEKEYOK")){
			
			System.exit(0);
			
		}
	}
}