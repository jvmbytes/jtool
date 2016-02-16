/**
 * Created By: Comwave Project Team Created Date: 2015年7月10日
 */
package jtool.sql.domain;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class Column {

  String name;

  String type;

  public int size;

  int decimalDigits;

  boolean nullable;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getDecimalDigits() {
    return decimalDigits;
  }

  public void setDecimalDigits(int decimalDigits) {
    this.decimalDigits = decimalDigits;
  }

  public boolean isNullable() {
    return nullable;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  public int getIntegerSize() {
    return size - decimalDigits;
  }

}
