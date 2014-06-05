package org.ankus;

import org.ankus.util.AnkusUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;


/**
 * Created with IntelliJ IDEA.
 * User: suhyunjeon
 * Date: 13. 8. 21.
 * Time: AM 9:22
 * To change this template use File | Settings | File Templates.
 */
public class ReadFileFromHDFSTest {

    @Test
    public void test() throws IOException {

        Properties configProperties = AnkusUtils.getConfigProperties();
        System.out.println("con : "+configProperties);
        String tempDirectory = AnkusUtils.createDirectoryForHDFS("/output");
        System.out.println(tempDirectory);
    }
}
