README

This assignment has been coded in Java and does not require any make file for execution.

To compile the code, go to the "src" folder in the Submission and compile using "javac":
	javac *.java
This will create the .class files

To start a node, go to same folder (which contains the .class files, in this case "src" folder) and type the following command in the terminal:
	java NodeServer <BootStrap IP> <BootStrap Port> <Self Port>

This will start the node at the local host at “Self Port” and send registration request to bootstrap server.

The node also contains a console reader. 
self: Self Node Information
keytable: display keytable
fingertable : display fingertable
search: start search
leave: exit network
 
