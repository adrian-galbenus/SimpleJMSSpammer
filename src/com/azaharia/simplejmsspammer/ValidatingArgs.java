package com.azaharia.simplejmsspammer;

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Zaharia Costin-Alexandru, email: zaharia.c.alexandru@gmail.com on 9/6/2016.
 */

public class ValidatingArgs {
    private String nrConn;
    private String gwUri;
    private String gwMessage;
    private String typeOfSub;
    private String typeOfMsg;

    public ValidatingArgs(){
    }

    public ValidatingArgs(String nrConn, String gwUri, String gwMessage, String typeOfSub, String typeOfMsg) {
        this.nrConn = nrConn;
        this.gwUri = gwUri;
        this.gwMessage = gwMessage;
        this.typeOfSub = typeOfSub;
        this.typeOfMsg = typeOfMsg;
    }

    public boolean isValid(){
        if(!isInt(nrConn)){
            System.out.println("This is not an integer number!");
            return false;
        }
        if(!isIntInRange(nrConn)){
            System.out.println("The number is not in the range (1,600]");
            return false;
        }
        if(!regexUri(gwUri)){
            System.out.println("The URI you have entered is not valid!");
            return false;
        }
        return true;
    }


    private boolean isInt(String conn){
        try{
            Integer.parseInt(conn);
        } catch (NumberFormatException nfe){
            return false;
        }
        return true;
    }

    private boolean isIntInRange(String conn){
        if (Integer.parseInt(conn) < 0 || Integer.parseInt(conn) > 600){
            return false;
        }
        return true;
    }

    private boolean regexUri(String gwUri){
        String regex = "^(ws):[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        if(!IsMatch(gwUri,regex)){
            return false;
        }
        return true;
    }

    private static boolean IsMatch(String s, String pattern) {
        try {
            Pattern patt = Pattern.compile(pattern);
            Matcher matcher = patt.matcher(s);
            return matcher.matches();
        } catch (RuntimeException e) {
            return false;
        }
    }

    public int nrOfConnection(){
        return Integer.parseInt(nrConn);
    }
    public TypeOf getSubscription(){
        if(!typeOfSub.equals("-d")){
            return TypeOf.NONDURABLE;
        }
        return TypeOf.DURABLE;
    }
    public TypeOf getMessages(){
        if(!typeOfMsg.equals("-t")){
            return TypeOf.BINARY;
        }
        return TypeOf.TEXT;
    }

    public String getGwUri() {
        return gwUri;
    }

    public String getGwMessage() {
        if(gwMessage.equals("512")){
            char[] data = new char[512];
            Arrays.fill(data, 'a');
            gwMessage = new String(data);
        }
        return gwMessage;
    }
}
