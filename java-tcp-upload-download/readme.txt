#===============================================================
COMPLILE:
-------------
	javac TcpDownloadClient.java
	javac TcpDownloadServer.java
	javac TcpUploadClient.java
	javac TcpUploadServer.java
#===============================================================
UPLOAD A FILE TO SERVER:
-------------
	1. start a upload server on port 5000 to wait the client to upload
		java TcpUploadServer 5000 /tmp/1.zip
	2. connect to the server and send a file
		java TcpUploadClient 127.0.0.1 5000 c:/1.zip
#===============================================================
DOWNLOAD A FILE FROM SERVER:
-------------
	1.start a listen to wait client to connect and download a file
		java TcpDownloadServer 5000 /tmp/1.zip
	2.connect to server to download the file
		java TcpDownloadClient 127.0.0.1 5000 d:/1.zip
		
#===============================================================
TROUBLE SHOOTING:
-------------
1. Config environment:
	set PATH=c:/java/jdk/bin;%PATH%
	set classpath=.
	
	export PATH=/opt/java/jdk/bin:$PATH
	export classpath=.
