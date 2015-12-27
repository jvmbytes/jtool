/**
 * Created Date: Dec 27, 2015
 */
package http;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Enumeration;
import java.util.StringTokenizer;

/**
 * The Class HttpPostServer.
 * 
 * @author Geln Yang
 * @version 1.0
 */
public class HttpPostServer extends Thread {

  /** The Constant HTML_START. */
  static final String HTML_START =
      "<html><title>Java HTTP Server for upload and download</title><body>";
  
  /** The Constant HTML_END. */
  static final String HTML_END = "</body></html>";
  
  /** The server port. */
  static int serverPort = 5000;

  /** The client socket. */
  Socket clientSocket = null;

  /**
   * Instantiates a new http post server.
   * 
   * @param client the client
   */
  public HttpPostServer(Socket client) {
    clientSocket = client;
  }

  /**
   * Run.
   */
  public void run() {
    System.out.println("---------------------------------------------");
    String currentLine = null;
    String filename = null;
    String contentLength = null;
    PrintWriter printWriter = null;

    try {
      System.out.println("The Client " + clientSocket.getInetAddress() + ":"
          + clientSocket.getPort() + " is connected");

      BufferedReader inFromClient =
          new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      DataOutputStream outToClient = new DataOutputStream(clientSocket.getOutputStream());

      currentLine = inFromClient.readLine();
      String headerLine = currentLine;
      System.out.println("headerLine:" + headerLine);
      StringTokenizer tokenizer = new StringTokenizer(headerLine);
      String httpMethod = tokenizer.nextToken();
      String httpQueryString = tokenizer.nextToken();

      /*----------------GET-------------------------*/
      if (httpMethod.equals("GET")) {
        System.out.println("GET request");
        if (httpQueryString.equals("/")) {
          sendRequestPage(outToClient);

        } else {
          sendResponse(outToClient, 404, "<b>The Requested resource not found ...."
              + "Usage: http://127.0.0.1:5000</b>", false);
        }
      }

      /*----------------POST-------------------------*/
      else {
        System.out.println("POST request");
        do {
          currentLine = inFromClient.readLine();
          if (currentLine.indexOf("Content-Type: multipart/form-data") != -1) {
            String boundary = currentLine.split("boundary=")[1];

            /* ----------- get content length -------------- */
            while (true) {
              currentLine = inFromClient.readLine();
              if (currentLine.equals("")) {
                contentLength = "1";
                break;
              }
              if (currentLine != null && currentLine.indexOf("Content-Length:") != -1) {
                contentLength = currentLine.split(" ")[1];
                System.out.println("Content Length = " + contentLength);
                break;
              }
            }

            // Content length should be < 2MB
            // if (Long.valueOf(contentLength) > 2000000L) {
            // sendResponse(200, "File size should be < 2MB", false);
            // }

            /* ----------- get file name and content type -------------- */
            while (true) {
              currentLine = inFromClient.readLine();
              if (currentLine.indexOf("--" + boundary) != -1) {
                filename = inFromClient.readLine().split("filename=")[1].replaceAll("\"", "");
                String[] filelist = filename.split("\\" + System.getProperty("file.separator"));
                filename = filelist[filelist.length - 1];
                System.out.println("File to be uploaded = " + filename);
                break;
              }
            }
            String fileContentType = inFromClient.readLine().split(" ")[1];
            System.out.println("File content type = " + fileContentType);

            inFromClient.readLine();

            /* ----------- write content to file -------------- */
            printWriter = new PrintWriter(filename);
            currentLine = inFromClient.readLine();

            // Here we upload the actual file contents
            while (true) {
              if (currentLine.equals("--" + boundary + "--")) {
                break;
              } 
              printWriter.print(currentLine);
              currentLine = inFromClient.readLine();
            }

            /* ----------- send response -------------- */
            sendResponse(outToClient, 200, "File " + filename + " Uploaded..", false);
            printWriter.close();
          } // if
        } while (inFromClient.ready()); // End of do-while
      }// else
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Send request page.
   * 
   * @param outToClient the out to client
   * @throws Exception the exception
   */
  private void sendRequestPage(DataOutputStream outToClient) throws Exception {
    StringBuffer buffer = new StringBuffer();
    buffer.append(HttpPostServer.HTML_START);
    buffer.append("<script language=\"text/javascript\">");
    buffer.append("function setAction(ip){");
    buffer
        .append("document.getElementById(\"\").action=\"http://\" + ip + \":" + serverPort + "\"");
    buffer.append("}");
    buffer.append("</script>");
    String ip = null;
    Enumeration<NetworkInterface> networks = NetworkInterface.getNetworkInterfaces();
    while (networks.hasMoreElements()) {
      NetworkInterface network = networks.nextElement();
      Enumeration<InetAddress> addresses = network.getInetAddresses();
      while (addresses.hasMoreElements()) {
        InetAddress address = addresses.nextElement();
        String addr = address.getHostAddress();
        if (addr.matches("\\d+(\\.\\d+){3}")) {
          ip = addr;
          buffer.append("<input type=\"radio\" checked name=\"ip\" value=\"" + ip
              + "\" onclick=\"javascript:setAction(this.value)\"/>" + ip + "<br/>");
        }
      }
    }
    buffer.append("CURRENTLY ONLY SUPPORT TEXT TO UPLOAD!<br>");
    buffer.append("<form action=\"http://" + ip + ":" + serverPort
        + "\" enctype=\"multipart/form-data\"" + "method=\"post\">");
    buffer.append("Enter the name of the File <input name=\"file\" type=\"file\"><br>");
    buffer.append("<input value=\"Upload\" type=\"submit\"></form>");
    buffer.append(HttpPostServer.HTML_END);
    sendResponse(outToClient, 200, buffer.toString(), false);
  }

  /**
   * Send response.
   * 
   * @param outToClient the out to client
   * @param statusCode the status code
   * @param responseString the response string
   * @param isFile the is file
   * @throws Exception the exception
   */
  public void sendResponse(DataOutputStream outToClient, int statusCode, String responseString,
      boolean isFile) throws Exception {
    String statusLine = null;
    String serverdetails = "Server: Java HTTPServer";
    String contentLengthLine = null;
    String fileName = null;
    String contentTypeLine = "Content-Type: text/html" + "\r\n";
    FileInputStream fin = null;

    if (statusCode == 200)
      statusLine = "HTTP/1.1 200 OK" + "\r\n";
    else
      statusLine = "HTTP/1.1 404 Not Found" + "\r\n";

    if (isFile) {
      fileName = responseString;
      fin = new FileInputStream(fileName);
      contentLengthLine = "Content-Length: " + Integer.toString(fin.available()) + "\r\n";
      if (!fileName.endsWith(".htm") && !fileName.endsWith(".html"))
        contentTypeLine = "Content-Type: \r\n";
    } else {
      responseString = HttpPostServer.HTML_START + responseString + HttpPostServer.HTML_END;
      contentLengthLine = "Content-Length: " + responseString.length() + "\r\n";
    }

    outToClient.writeBytes(statusLine);
    outToClient.writeBytes(serverdetails);
    outToClient.writeBytes(contentTypeLine);
    outToClient.writeBytes(contentLengthLine);
    outToClient.writeBytes("Connection: close\r\n");
    outToClient.writeBytes("\r\n");

    if (isFile) {
      sendFile(fin, outToClient);
    } else {
      outToClient.writeBytes(responseString);
    }
    outToClient.close();
  }

  /**
   * Send file.
   * 
   * @param fin the fin
   * @param out the out
   * @throws Exception the exception
   */
  public void sendFile(FileInputStream fin, DataOutputStream out) throws Exception {
    byte[] buffer = new byte[1024];
    int bytesRead;

    while ((bytesRead = fin.read(buffer)) != -1) {
      out.write(buffer, 0, bytesRead);
    }
    fin.close();
  }

  /**
   * The main method.
   * 
   * @param args the arguments
   * @throws Exception the exception
   */
  @SuppressWarnings("resource")
  public static void main(String args[]) throws Exception {

    if (args.length > 0) {
      serverPort = Integer.valueOf(args[0]);
    }
    ServerSocket server = new ServerSocket(serverPort);
    System.out.println("HTTP Server Waiting for client on port " + serverPort);

    while (true) {
      Socket connected = server.accept();
      (new HttpPostServer(connected)).start();
    }
  }
}
