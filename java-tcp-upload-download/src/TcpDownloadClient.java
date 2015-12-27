/**
 * Created Date: Dec 27, 2015
 */
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.Socket;

/**
 * The Class TcpDownloadClient.
 * 
 * @author Geln Yang
 * @version 1.0
 */
public class TcpDownloadClient {

  /**
   * The main method.
   * 
   * @param args the arguments
   * @throws Exception the exception
   */
  public static void main(String args[]) throws Exception {
    String serverIP = args[0];
    int serverPort = Integer.parseInt(args[1]);
    String saveFilePath = args[2];

    System.out.println("--------------------------------------");
    System.out.println("start to connecet to " + serverIP + ":" + serverPort);
    Socket serverSocket = new Socket(serverIP, serverPort);
    InputStream is = serverSocket.getInputStream();

    if (is != null) {
      byte[] bytes = new byte[10240];
      int size;
      FileOutputStream fos = new FileOutputStream(saveFilePath);
      System.out.println("start to download ...");
      System.out.println();
      while ((size = is.read(bytes, 0, bytes.length)) != -1) {
        fos.write(bytes, 0, size);
        System.out.print(".");
      }
      System.out.println();
      System.out.println("download finish, save file to " + saveFilePath);
      fos.close();
      serverSocket.close();
    }
  }
}
