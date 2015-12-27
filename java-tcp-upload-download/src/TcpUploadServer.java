/**
 * @author Geln Yang
 * @version 1.0
 */
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * The Class TcpUploadServer.
 */
public class TcpUploadServer {

  /**
   * The main method.
   * 
   * @param args the arguments
   * @throws Exception the exception
   */
  public static void main(String args[]) throws Exception {
    int port = Integer.parseInt(args[0]);
    String saveFilePath = args[1];
    System.out.println("--------------------------------------");
    System.out.println("start to wait client to upload file and save it to " + saveFilePath);

    ServerSocket serverSocket = new ServerSocket(port);
    Socket connectionSocket = serverSocket.accept();
    InputStream is = connectionSocket.getInputStream();

    System.out.println("connection setup, start to transfer data ...");
    byte[] bytes = new byte[10240];
    int size;
    FileOutputStream fos = new FileOutputStream(saveFilePath);

    System.out.println();
    while ((size = is.read(bytes, 0, bytes.length)) != -1) {
      fos.write(bytes, 0, size);
      System.out.print(".");
    }
    System.out.println();
    fos.close();
    serverSocket.close();
    System.out.println("upload finish");
  }
}
