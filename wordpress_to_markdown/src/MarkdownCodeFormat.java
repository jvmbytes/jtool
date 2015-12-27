import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * Created Date: Nov 23, 2015
 */

/**
 * @author Geln Yang
 * @version 1.0
 */
public class MarkdownCodeFormat {

  private static final char SCHAR = '`';
  private static final Object FOUR_BLANK = "    ";

  public static void main(String[] args) throws IOException {
    String path = "F:\\git_workspace\\github_com\\exitwp\\wordpress-xml\\wordpress.xml";
    File file = new File(path);
    String encoding = "UTF-8";
    String content = FileUtils.readFileToString(file, encoding);
    StringBuffer in = new StringBuffer();
    in.append(content);
    int length = content.length();
    int index = 0;
    boolean startReplaceFlag = false;

    while (index < length) {
      char c = in.charAt(index);
      if (c == SCHAR && in.charAt(index + 1) == SCHAR && in.charAt(index + 2) == SCHAR) {
        startReplaceFlag = !startReplaceFlag;
        index += 3;
        continue;
      }

      if (startReplaceFlag && c == '\n') {
        in.insert(index + 1, FOUR_BLANK);
        length += 4;
        index += 4;
      }
      index++;
    }

    FileUtils.writeStringToFile(new File(path + ".new"), in.toString(), encoding);

    System.out.println("over");
  }
}
