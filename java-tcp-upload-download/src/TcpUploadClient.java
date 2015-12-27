/**
 * Created Date: Dec 27, 2015
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.Socket;


/**
 * The Class TcpUploadClient.
 * 
 * @author Geln Yang
 * @version 1.0
 */
public class TcpUploadClient {

  /**
   * The main method.
   * 
   * @param args the arguments
   * @throws Exception the exception
   */
  @SuppressWarnings("resource")
  public static void main(String args[]) throws Exception {
    String serverIP = args[0];
    int serverPort = Integer.parseInt(args[1]);
    String uploadFilePath = args[2];
    System.out.println("--------------------------------------");
    System.out.println("start to connecet to " + serverIP + ":" + serverPort);
    Socket clientSocket = new Socket(serverIP, serverPort);
    OutputStream os = clientSocket.getOutputStream();

    System.out.println("start to upload file " + uploadFilePath);
    File file = new File(uploadFilePath);
    byte[] bytes = new byte[10240];
    int size = 0;

    FileInputStream fis = new FileInputStream(file);
    System.out.println();
    while ((size = fis.read(bytes, 0, bytes.length)) != -1) {
      os.write(bytes, 0, size);
      System.out.print(".");
    }
    System.out.println();
    os.flush();
    os.close();
    clientSocket.close();
    System.out.println("upload finish!");
  }
}
