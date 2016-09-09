package com.azaharia.simplejmsspammer;


import com.kaazing.gateway.jms.client.*;
import com.kaazing.net.http.HttpRedirectPolicy;
import com.kaazing.net.ws.WebSocketFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by AZaharia on 6/8/2016.
 */
public class Main {


    public static void main(String[] args) throws JMSException, InterruptedException, NamingException {
        ValidatingArgs validate;
        if(args.length ==3) {
            validate = new ValidatingArgs();
        } else{
            throw new IllegalArgumentException("Invalid number of arguments: correct usage of program is : [nr. of connections]  [gateway URI]  [\"message to be sent\"]");
        }

        // Create a new WebSocket object
        if(validate.isValid(args[0], args[1], args[2])) {
            for (int i = 0; i < Integer.parseInt(args[0]); i++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            connectAndSend(args[1], args[2]);
                        } catch (NamingException e) {
                            e.printStackTrace();
                        } catch (JMSException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }
        }
    }

    private static void connectAndSend(String gwUri, String gwMessage) throws NamingException, JMSException, InterruptedException {


        JmsConnectionFactory theGatewayConnectionFactory = null;
        InitialContext ctx;
        Connection theGatewayConnection = null;
        Session theGatewaySession = null;
        String myClientId = UUID.randomUUID().toString();


        //Initial context is need to avoid NullPointerException
        Properties props = new Properties();
        props.put(InitialContext.INITIAL_CONTEXT_FACTORY, "com.kaazing.gateway.jms.client.JmsInitialContextFactory");
        ctx = new InitialContext(props);
        ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");

        if (connectionFactory instanceof JmsConnectionFactory) {
            try {
                ((JmsConnectionFactory) connectionFactory).setGatewayLocation(new URI(gwUri));
                WebSocketFactory webSocketFactory = ((JmsConnectionFactory) connectionFactory).getWebSocketFactory();
                webSocketFactory.setDefaultRedirectPolicy(HttpRedirectPolicy.SAME_DOMAIN);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("EXCEPTION: " + e.getMessage());
            }

        }

        theGatewayConnection = connectionFactory.createConnection(null, null);
        theGatewayConnection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException arg0) {
                arg0.printStackTrace();
            }
        });

        try {
            theGatewaySession = theGatewayConnection.createSession(false, theGatewaySession.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            System.out.println("EXCEPTION 3: " + e.getMessage());
        }

        theGatewayConnection.start();

        //Queue jmsTopic = (Queue) ctx.lookup("/queue/Q1");
        Topic jmsTopic = (Topic) ctx.lookup("/topic/destination");

        MessageConsumer consumer = theGatewaySession.createConsumer(jmsTopic);

        //char[] data = new char[1024];
        //Arrays.fill(data, 'a');
        //String str = new String(data);
        String strMessage = gwMessage;

        //MessageProducer producer = theGatewaySession.createProducer(jmsTopic);
        //BytesMessage bytesMessage = theGatewaySession.createBytesMessage();
        //bytesMessage.writeUTF(str);

        TextMessage message = theGatewaySession.createTextMessage(strMessage);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    //BytesMessage bytesMessage = (BytesMessage)message;

                    //long len = bytesMessage.getBodyLength();
                    //byte b[] = new byte[(int)len];
                    //bytesMessage.readBytes(b);
                    //System.out.println("MESSAGE: " + hexDump(b).length() + " -> " + timestampt());
                    System.out.println("MESSAGE: " + ((TextMessage) message).getText() + " -> "  + timestampt());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

/*        //DEBUG
        // Create a MessageConsumer for the specified destination.
        consumer = theGatewaySession.createConsumer(jmsTopic);
        // Set the session's distinguished message listener.
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("RECEIVED MESSAGE: " + ((TextMessage) message).getText());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });*/
        //DEBUG - Iterator to sent 3 messages

        MessageProducer producer = theGatewaySession.createProducer(jmsTopic);
        System.out.println("SENDING MESSAGES: " + Thread.currentThread() + timestampt());
        while(true) {
            //producer.send(message);
            Thread.sleep(3000);
            producer.send(message);
        }
    }

    public static String hexDump(byte[] b) {
        if (b.length == 0) {
            return "empty";
        }

        StringBuilder out = new StringBuilder();
        for (int i=0; i < b.length; i++) {
            out.append(Integer.toHexString(b[i]));
        }
        return out.toString();
    }

    private static String timestampt(){
        String timeStamp;
        Date date= new java.util.Date();
        return timeStamp = new SimpleDateFormat("HH:mm:ss.ms").format(new Date());
    }

}
