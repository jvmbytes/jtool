/**
 * Created By: Comwave Project Team Created Date: Aug 16, 2011 8:58:12 PM
 */
package jtool.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.hslf.model.OLEShape;
import org.apache.poi.hslf.model.Shape;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.usermodel.SlideShow;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ExcelUtil {

    private static final Logger logger = LoggerFactory.getLogger(ExcelUtil.class);

    /**
     * @param file
     * @return
     * @throws IOException
     * @throws
     * @throws
     */
    public static XSSFWorkbook readExcel(File file) throws IOException {
        InputStream inp = new FileInputStream(file);
        // If clearly doesn't do mark/reset, wrap up
        if (!inp.markSupported()) {
            inp = new PushbackInputStream(inp, 8);
        }
        try {
            return new XSSFWorkbook(OPCPackage.open(inp));
        } catch (InvalidFormatException e) {
            return new XSSFWorkbook(new FileInputStream(file));
        }
    }

    public static HSSFWorkbook readExcel2003(File file) throws IOException {
        return new HSSFWorkbook(new FileInputStream(file));
    }

    /**
     * 讀取excel檔案內容
     * 
     * @param workbook
     * @param sheetIndex
     * @param fromRowIndex
     * @param toRowIndex
     * @return
     * @throws Exception
     */
    public static List<List<Object>> readExcelLines(XSSFWorkbook workbook, int sheetIndex, int fromRowIndex, int toRowIndex) throws IOException {
        if (fromRowIndex > toRowIndex && toRowIndex >= 0) {
            throw new IOException("The fromRowIndex is great than toRowIndex!");
        }
        XSSFSheet sheet = workbook.getSheetAt(sheetIndex);
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        if (null != sheet) {
            int maxRowNum = sheet.getLastRowNum();
            if (toRowIndex < 0) {
                toRowIndex = maxRowNum;
            }
            if (toRowIndex < 0 || maxRowNum < toRowIndex) {
                toRowIndex = maxRowNum;
                // throw new
                // IOException("The toRowIndex is great than the max row num " +
                // maxRowNum);
            }
            List<List<Object>> lines = new ArrayList<List<Object>>();
            for (int rowIndex = fromRowIndex; rowIndex <= toRowIndex; rowIndex++) {
                if (null != sheet.getRow(rowIndex)) {
                    XSSFRow row = sheet.getRow(rowIndex);
                    int cellCount = row.getLastCellNum();
                    List<Object> line = new ArrayList<Object>();
                    lines.add(line);
                    for (int cellIndex = 0; cellIndex < cellCount; cellIndex++) {
                        XSSFCell cell = row.getCell(cellIndex);
                        Object cellVal = "";
                        if (null != cell) {
                            cellVal = getCellValue(cell, evaluator);
                        }
                        line.add(cellVal);
                    }
                }
            }// END for

            return lines;
        } else {
            logger.warn("no sheet index is " + sheetIndex);
        }
        logger.warn("no sheet found!");
        return null;
    }

    /**
     * 讀取excel檔案內容
     * 
     * @param workbook
     * @param sheetIndex
     * @param fromRowIndex
     * @param toRowIndex
     * @return
     * @throws Exception
     */
    public static List<List<Object>> readExcelLines2003(HSSFWorkbook workbook, int sheetIndex, int fromRowIndex, int toRowIndex) throws IOException {
        if (fromRowIndex > toRowIndex && toRowIndex >= 0) {
            throw new IOException("The fromRowIndex is great than toRowIndex!");
        }
        HSSFSheet sheet = workbook.getSheetAt(sheetIndex);
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        if (null != sheet) {
            int maxRowNum = sheet.getLastRowNum();
            if (toRowIndex < 0) {
                toRowIndex = maxRowNum;
            }
            if (maxRowNum < toRowIndex) {
                toRowIndex = maxRowNum;
                // throw new
                // IOException("The toRowIndex is great than the max row num " +
                // maxRowNum);
            }
            List<List<Object>> lines = new ArrayList<List<Object>>();
            for (int rowIndex = fromRowIndex; rowIndex <= toRowIndex; rowIndex++) {
                if (null != sheet.getRow(rowIndex)) {
                    HSSFRow row = sheet.getRow(rowIndex);
                    int cellCount = row.getLastCellNum();
                    List<Object> line = new ArrayList<Object>();
                    lines.add(line);
                    for (int cellIndex = 0; cellIndex < cellCount; cellIndex++) {
                        HSSFCell cell = row.getCell(cellIndex);
                        Object cellVal = "";
                        if (null != cell) {
                            cellVal = getCellValue2003(cell, evaluator);
                        }
                        line.add(cellVal);
                    }
                }
            }// END for

            return lines;
        } else {
            logger.warn("no sheet index is " + sheetIndex);
        }
        logger.warn("no sheet found!");
        return null;
    }

    /**
     * @param cell
     * @param evaluator
     * @return
     */
    private static Object getCellValue(XSSFCell cell, FormulaEvaluator evaluator) {
        DataFormatter dataFormatter = new DataFormatter();
        int cellType = cell.getCellType();
        Object cellVal = null;
        switch (cellType) {
            case Cell.CELL_TYPE_NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    cellVal = cell.getDateCellValue();
                } else {
                    cellVal = cell.getNumericCellValue();
                }
                break;
            case Cell.CELL_TYPE_STRING:
                cellVal = cell.getStringCellValue();
                break;
            case Cell.CELL_TYPE_BOOLEAN:
                cellVal = cell.getBooleanCellValue();
                break;
            case Cell.CELL_TYPE_FORMULA:
                try {
                    cellVal = dataFormatter.formatCellValue(cell, evaluator);
                } catch (Exception e) {
                    if (e.getMessage().indexOf("not implemented yet") != -1) {
                        logger.warn(e.getMessage());
                    } else {
                        throw new RuntimeException(e);
                    }
                }

                break;
            default:
                cellVal = cell.getStringCellValue();
        }
        return cellVal;
    }

    /**
     * @param cell
     * @param evaluator
     * @return
     */
    private static Object getCellValue2003(HSSFCell cell, FormulaEvaluator evaluator) {
        DataFormatter dataFormatter = new DataFormatter();
        int cellType = cell.getCellType();
        Object cellVal = null;
        switch (cellType) {
            case Cell.CELL_TYPE_NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    cellVal = cell.getDateCellValue();
                } else {
                    cellVal = cell.getNumericCellValue();
                }
                break;
            case Cell.CELL_TYPE_STRING:
                cellVal = cell.getStringCellValue();
                break;
            case Cell.CELL_TYPE_BOOLEAN:
                cellVal = cell.getBooleanCellValue();
                break;
            case Cell.CELL_TYPE_FORMULA:
                try {
                    cellVal = dataFormatter.formatCellValue(cell, evaluator);
                } catch (Exception e) {
                    if (e.getMessage().indexOf("Could not resolve external workbook name") != -1 || e.getMessage().indexOf("Unexpected celltype (5)") != -1) {
                        logger.warn(e.getMessage());
                    } else {
                        throw new RuntimeException(e);
                    }
                }
                break;
            default:
                cellVal = cell.getStringCellValue();
        }
        return cellVal;
    }

    /**
     * 從ppt中賺取excel
     */
    @SuppressWarnings("resource")
    public static File fetchExcelFromPPT(File pptFile, int slideIndex, int shapeIndex) throws IOException {
        SlideShow ppt = new SlideShow(new FileInputStream(pptFile));

        Slide[] slides = ppt.getSlides();
        logger.debug("Slide count: " + slides.length);

        for (int i = 0; i < slides.length; i++) {
            if (i != slideIndex) {
                continue;
            }
            Slide slide = slides[i];
            logger.debug("Slide NO:" + slide.getSlideNumber());
            String excelTitle = slide.getTitle();
            excelTitle = excelTitle.replace("\r", "").replace("\n", "");
            logger.debug(slide.getTitle());
            Shape[] shapes = slide.getShapes();
            logger.debug("shapes length:" + shapes.length);

            for (int j = 0; j < shapes.length; j++) {

                if (shapeIndex >= 0 && j != shapeIndex) {
                    continue;
                }

                Shape shape = shapes[j];
                if (shape instanceof OLEShape) {
                    OLEShape oleShape = (OLEShape) shape;
                    logger.debug(oleShape.getInstanceName());
                    logger.debug(oleShape.getFullName());

                    HSSFWorkbook wb = new HSSFWorkbook(oleShape.getObjectData().getData());
                    String excelFileName = FilenameUtils.getBaseName(pptFile.getName()) + "_" + slideIndex + "_" + excelTitle + ".xls";
                    File excel = new File(pptFile.getParent() + File.separator + excelFileName);
                    logger.debug("write file: " + excel.getAbsolutePath());
                    wb.write(new FileOutputStream(excel));
                    return excel;
                }
                if (shapeIndex >= 0) {
                    break; // END
                }
            }

            break;// END
        }
        logger.warn("no excel found!");
        return null;

    }

    public static File fetchExcelFromPPT(File pptFile, int slideIndex) throws IOException {
        return fetchExcelFromPPT(pptFile, slideIndex, -1);
    }

    public static List<List<Object>> readExcelLines(File file, int sheetIndex, int fromRowIndex, int toRowIndex) throws IOException {
        Workbook excel;
        try {
            excel = readExcel2003(file);
        } catch (Exception e) {
            logger.debug(e.getMessage());
            excel = readExcel(file);
        }
        if (excel instanceof XSSFWorkbook) {
            return readExcelLines((XSSFWorkbook) excel, sheetIndex, fromRowIndex, toRowIndex);
        } else if (excel instanceof HSSFWorkbook) {
            return readExcelLines2003((HSSFWorkbook) excel, sheetIndex, fromRowIndex, toRowIndex);
        }
        return null;
    }

    public static List<List<Object>> readExcelLines(File workbook, int sheetIndex, int fromRowIndex) throws IOException {
        return readExcelLines(workbook, sheetIndex, fromRowIndex, -1);
    }

    public static Workbook create(InputStream inp) throws IOException {
        try {
            // If clearly doesn't do mark/reset, wrap up
            if (!inp.markSupported()) {
                inp = new PushbackInputStream(inp, 8);
            }

            if (POIFSFileSystem.hasPOIFSHeader(inp)) {
                return new HSSFWorkbook(inp);
            }
            if (POIXMLDocument.hasOOXMLHeader(inp)) {
                return new XSSFWorkbook(OPCPackage.open(inp));
            }
            throw new IllegalArgumentException("Your InputStream was neither an OLE2 stream, nor an OOXML stream");
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    public static int getNumberOfSheets(File file) throws IOException {
        Workbook excel;
        try {
            excel = readExcel2003(file);
        } catch (Exception e) {
            logger.debug(e.getMessage());
            excel = readExcel(file);
        }
        return excel.getNumberOfSheets();

    }
}
