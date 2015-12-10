/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.phd.mongolucene.utils;

/**
 *
 * @author aka
 */
public class CStringUtils {
    public static Boolean isEmpty(String s){
		if(s==null || s.trim().length()==0){
			return true;
		}
		return false;
	}
}
