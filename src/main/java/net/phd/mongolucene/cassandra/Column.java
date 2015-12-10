/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.phd.mongolucene.cassandra;

import java.io.Serializable;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author aka
 */
@XmlRootElement
public class Column implements Serializable{
    public String id = "ID";
    public String lname = "ID";
    public String fname = "ID";

    public Column() {
    }

    public Column(String id, String fname, String lname) {
        this.id = id;
        this.fname = fname;
        this.lname = lname;
    }
    
}
