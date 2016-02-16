/**
 * Created By: Comwave Project Team Created Date: 2015年7月26日
 */
package jtool.sql.domain;


/**
 * @author Geln Yang
 * @version 1.0
 */
public interface DataHolder {

  public int getSize();

  public RowHolder getRow(int i);
}
