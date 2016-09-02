package com.azaharia.simplejmsspammer;


import com.kaazing.gateway.jms.client.*;
import com.kaazing.net.http.HttpRedirectPolicy;
import com.kaazing.net.ws.WebSocketFactory;
import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by AZaharia on 6/8/2016.
 */
public class Main {


    public static void main(String[] args) throws JMSException, InterruptedException, NamingException {

        // Create a new WebSocket object
        for (int i = 0; i < 200; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        connectAndSend();
                        System.out.println(Thread.currentThread());
                    } catch (NamingException e) {
                        e.printStackTrace();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    private static void connectAndSend() throws NamingException, JMSException {


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
                ((JmsConnectionFactory) connectionFactory).setGatewayLocation(new URI("ws://10.2.56.25:8001/jms"));
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

        Topic jmsTopic = (Topic) ctx.lookup("/topic/destination");

        MessageConsumer consumer = theGatewaySession.createConsumer(jmsTopic);

        char[] data = new char[1024];
        Arrays.fill(data, 'a');
        String str = new String(data);

        MessageProducer producer = theGatewaySession.createProducer(jmsTopic);
        BytesMessage bytesMessage = theGatewaySession.createBytesMessage();
        bytesMessage.writeUTF(str);

        TextMessage message = theGatewaySession.createTextMessage(hexDump(str.getBytes()) + Thread.currentThread());
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    BytesMessage bytesMessage = (BytesMessage)message;

                    long len = bytesMessage.getBodyLength();
                    byte b[] = new byte[(int)len];
                    bytesMessage.readBytes(b);
                    System.out.println("MESSAGE: " + hexDump(b).length());
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
        while(true) {
            //producer.send(message);

            producer.send(bytesMessage);
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

}