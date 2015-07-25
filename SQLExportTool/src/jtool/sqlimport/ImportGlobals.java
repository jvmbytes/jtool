/**
 * Created By: Comwave Project Team Created Date: 2015年7月25日
 */
package jtool.sqlimport;

/**
 * @author Geln Yang
 * @version 1.0
 */
public final class ImportGlobals {

    private static boolean continueWhenError = false;

    private static boolean continueWhenPkFkError = false;

    private static int batchsize = 2000;

    private static boolean autoCommit = false;

    public static boolean isContinueWhenError() {
        return continueWhenError;
    }

    public static void setContinueWhenError(boolean continueWhenError) {
        ImportGlobals.continueWhenError = continueWhenError;
    }

    public static boolean isContinueWhenPkFkError() {
        return continueWhenPkFkError;
    }

    public static void setContinueWhenPkFkError(boolean continueWhenPkFkError) {
        ImportGlobals.continueWhenPkFkError = continueWhenPkFkError;
    }

    public static int getBatchsize() {
        return batchsize;
    }

    public static void setBatchsize(int batchsize) {
        ImportGlobals.batchsize = batchsize;
    }

    public static boolean isAutoCommit() {
        return autoCommit;
    }

    public static void setAutoCommit(boolean autoCommit) {
        ImportGlobals.autoCommit = autoCommit;
    }

}
