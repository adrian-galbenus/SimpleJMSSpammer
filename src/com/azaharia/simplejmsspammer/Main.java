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
    private static ValidatingArgs validate;


    public static void main(String[] args) throws JMSException, InterruptedException, NamingException {

        if(args.length == 3){
            args[3] = "-nd";
            args[4] ="-t";
            validate = new ValidatingArgs(args[0], args[1], args[2], args[3], args[4]);
        }else if(args.length ==5) {
            validate = new ValidatingArgs(args[0], args[1], args[2], args[3], args[4]);
        } else{
            throw new IllegalArgumentException("Invalid number of arguments: correct usage of program is : [nr. of connections]  [gateway URI]  " +
                    "[\"message to be sent\" - if message is 512 will send 1kb of data, any other message will be sent as is.] [type of subscription: \"-d\" \"-nd\"] [type of message: \"-t\" \"-b\"]");
        }

        // Create a new WebSocket object
        if(validate.isValid()) {

            for (int i = 0; i < validate.nrOfConnection(); i++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            MyConnection myConnection = new MyConnection(validate.getGwUri(),validate.getGwMessage(),validate.getSubscription(),validate.getMessages());
                            myConnection.connectAndSendDurable();
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

/*    private static void connectAndSend(String gwUri, String gwMessage) throws NamingException, JMSException, InterruptedException {


        JmsConnectionFactory theGatewayConnectionFactory = null;
        InitialContext ctx;
        Connection connection = null;
        Session session = null;
        String myClientId = UUID.randomUUID().toString();
        Destination subDestinaton;
        MessageConsumer consumer;


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

        connection = connectionFactory.createConnection(null, null);
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException arg0) {
                arg0.printStackTrace();
            }
        });

        try {
            session = connection.createSession(false, session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            System.out.println("EXCEPTION 3: " + e.getMessage());
        }

        connection.start();

        //Queue jmsTopic = (Queue) ctx.lookup("/queue/Q1");
        Topic jmsTopic = (Topic) ctx.lookup("/topic/destination");

        consumer = session.createConsumer(jmsTopic);

        //char[] data = new char[512];
        //Arrays.fill(data, 'a');
        //String str = new String(data);
        String strMessage = gwMessage;

        //MessageProducer producer = session.createProducer(jmsTopic);
        //BytesMessage bytesMessage = session.createBytesMessage();
        //bytesMessage.writeUTF(str);

        TextMessage message = session.createTextMessage(strMessage);
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

//        //DEBUG
//        // Create a MessageConsumer for the specified destination.
//        consumer = session.createConsumer(jmsTopic);
//        // Set the session's distinguished message listener.
//        consumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message message) {
//                try {
//                    System.out.println("RECEIVED MESSAGE: " + ((TextMessage) message).getText());
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
        //DEBUG - Iterator to sent 3 messages

        MessageProducer producer = session.createProducer(jmsTopic);
        System.out.println("SENDING MESSAGES: " + Thread.currentThread() + timestampt());
        while(true) {
            //producer.send(message);
            Thread.sleep(3000);
            producer.send(message);
        }
    }

*/
/*
    private static void connectAndSendDurable(String gwUri, String gwMessage) throws NamingException, JMSException, InterruptedException {


        JmsConnectionFactory theGatewayConnectionFactory = null;
        InitialContext ctx;
        Connection connection = null;
        Session session = null;
        String myClientId = UUID.randomUUID().toString();
        Destination subDestinaton;
        MessageConsumer consumer;
        Topic topic = null;
        TypeOf type = TypeOf.TEXT;


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

        connection = connectionFactory.createConnection(null, null);
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException arg0) {
                arg0.printStackTrace();
            }
        });

        try {
            session = connection.createSession(false, session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            System.out.println("EXCEPTION 3: " + e.getMessage());
        }

        connection.start();

        String destinationName = "destination";
        String selectorText= null;
        String durableSubscibeName = "Alex";

        boolean hasDurableSubscriberName= durableSubscibeName != null && !durableSubscibeName.isEmpty();
        boolean hasSelector = selectorText!=null && !selectorText.isEmpty();

        System.out.println("SUBSCRIBED: " + destinationName + " " + durableSubscibeName + " " + selectorText);

        subDestinaton = (Destination) ctx.lookup("/topic/" + destinationName);

        if(hasDurableSubscriberName && subDestinaton instanceof Topic){
            topic = (Topic) subDestinaton;
            TopicSubscriber subscriber;
            if(hasSelector){
                subscriber = session.createDurableSubscriber(topic, durableSubscibeName, selectorText, false);
            } else{
                subscriber = session.createDurableSubscriber(topic, durableSubscibeName);
            }
            subscriber.setMessageListener(new MessageListener() {
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
        }

        String strMessage = gwMessage;

        //MessageProducer producer = session.createProducer(jmsTopic);
        //BytesMessage bytesMessage = session.createBytesMessage();
        //bytesMessage.writeUTF(str);

        TextMessage message = session.createTextMessage(strMessage);
//        consumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message message) {
//                try {
//                    //BytesMessage bytesMessage = (BytesMessage)message;
//
//                    //long len = bytesMessage.getBodyLength();
//                    //byte b[] = new byte[(int)len];
//                    //bytesMessage.readBytes(b);
//                    //System.out.println("MESSAGE: " + hexDump(b).length() + " -> " + timestampt());
//                    System.out.println("MESSAGE: " + ((TextMessage) message).getText() + " -> "  + timestampt());
//                } catch (JMSException e) {
//                    e.printStackTrace();
//                }
//            }
//        });

*/
/*        //DEBUG
        // Create a MessageConsumer for the specified destination.
        consumer = session.createConsumer(jmsTopic);
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
        });*//*

        //DEBUG - Iterator to sent 3 messages

        MessageProducer producer = session.createProducer(topic);
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
*/

}
