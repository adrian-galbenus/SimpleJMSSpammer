package com.azaharia.simplejmsspammer;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.exit;

/**
 * Created by Zaharia Costin-Alexandru, email: zaharia.c.alexandru@gmail.com on 9/6/2016.
 */
public class ValidatingArgs {
    private String nrConn = "";
    private String gwURI = "ws://localhost:8001/jms";
    private String gwMessage = "Standard message from client";

    public ValidatingArgs(String nrConn, String gwUri, String gwMessage){
        this.nrConn = nrConn;
        this.gwURI = gwUri;
        this.gwMessage = gwMessage;
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
        if(!regexUri(gwURI)){
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

/*    private boolean isValidateURI(String gwUri) {
        final URI uri;
        try {
            uri = new URI(gwUri);
        } catch (URISyntaxException e1) {
            System.out.println("DOES IT REACH THIS?!");
            return false;
        }
        return true;
    }*/


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

}
