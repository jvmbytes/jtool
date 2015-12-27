/**
 * Created Date: 2015年11月4日
 */

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * The Class TcpDownloadServer.
 * 
 * @author Geln Yang
 * @version 1.0
 */
public class TcpDownloadServer {

  /**
   * The main method.
   * 
   * @param args the arguments
   * @throws Exception the exception
   */
  @SuppressWarnings("resource")
  public static void main(String args[]) throws Exception {
    int port = Integer.parseInt(args[0]);
    String downloadFilePath = args[1];
    System.out.println("--------------------------------------");
    System.out.println("start server to wait client to connect and download file "
        + downloadFilePath);
    ServerSocket serverSocket = new ServerSocket(port);
    Socket connectionSocket = serverSocket.accept();
    BufferedOutputStream outToClient = new BufferedOutputStream(connectionSocket.getOutputStream());

    if (outToClient != null) {
      System.out.println("connection setup, start to download ...");

      File file = new File(downloadFilePath);
      byte[] bytes = new byte[10240];
      int size = 0;

      FileInputStream fis = new FileInputStream(file);

      System.out.println();
      while ((size = fis.read(bytes, 0, bytes.length)) != -1) {
        outToClient.write(bytes, 0, size);
        System.out.print(".");
      }
      System.out.println();

      outToClient.flush();
      outToClient.close();
      connectionSocket.close();
      System.out.println("download finish!");
    }
  }
}
