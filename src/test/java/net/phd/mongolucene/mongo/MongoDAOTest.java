/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.phd.mongolucene.mongo;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author aka
 */
public class MongoDAOTest {
    
    public MongoDAOTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of insertDataSet method, of class MongoDAO.
     */
    @org.junit.Test
    public void testInsertDataSet() {
       /* try {
            System.out.println("insertDataSet");
            MongoDAO instance = new MongoDAO();
            instance.insertDataSet(getFilePath(getFile()).toString());
                    
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    private File getFile() {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource("1200input.csv").getFile());
    }

    private Path getFilePath(File file) {
        Path path = Paths.get(file.toURI());
        return path;
    }
}
