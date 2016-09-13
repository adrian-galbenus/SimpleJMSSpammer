package com.azaharia.simplejmsspammer;

import com.kaazing.gateway.jms.client.JmsConnectionFactory;
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
 * Created by Zaharia Costin-Alexandru, email: zaharia.c.alexandru@gmail.com on 9/13/2016.
 */

public class MyConnection {
    private InitialContext ctx;
    private Connection connection = null;
    private Session session = null;
    private Destination subDestinaton;
    private MessageConsumer consumer;
    MessageProducer producer = null;
    MessageProducer mainProducer = null;
    TextMessage textMessage;
    BytesMessage bytesMessage;
    private Topic topic = null;
    private TypeOf typeOfSubcription = TypeOf.NONDURABLE;
    private TypeOf typeOfMessage = TypeOf.TEXT;
    private String gwUri;
    private String gwMessage;

    public MyConnection(String gwUri, String gwMessage, TypeOf typeOfSubcription, TypeOf typeOfMessage){
        this.gwUri = gwUri;
        this.gwMessage = gwMessage;
        this.typeOfSubcription = typeOfSubcription;
        this.typeOfMessage = typeOfMessage;
    }

    public void connectAndSendDurable() throws NamingException, JMSException, InterruptedException {

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
        //Connection has started

        switch (typeOfSubcription){
            case NONDURABLE:
                mainProducer = nonDurable();
                break;
            case DURABLE:
                mainProducer = durable();
                break;
            default:
                mainProducer = nonDurable();
                break;
        }
        while(true) {
            //producer.send(message);
            Thread.sleep(3000);
            switch (typeOfMessage){
                case TEXT:
                    textMessage = session.createTextMessage(gwMessage);
                    mainProducer.send(textMessage);
                    break;
                case BINARY:
                    bytesMessage = session.createBytesMessage();
                    bytesMessage.writeUTF(gwMessage);
                    mainProducer.send(bytesMessage);
                    break;
                default:
                    textMessage = session.createTextMessage(gwMessage);
                    mainProducer.send(textMessage);
                    break;
            }

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

    private MessageProducer durable() throws JMSException, NamingException, InterruptedException {
        String destinationName = "test1";
        String selectorText= null;
        String durableSubscibeName = UUID.randomUUID().toString().substring(0,8);

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
                        if(message instanceof BytesMessage) {
                            BytesMessage bytesMessage = (BytesMessage) message;
                            long len = bytesMessage.getBodyLength();
                            byte b[] = new byte[(int) len];
                            bytesMessage.readBytes(b);
                            System.out.println("MESSAGE: " + hexDump(b).length() + " -> " + timestampt());
                        }else{
                            System.out.println("MESSAGE: " + ((TextMessage) message).getText() + " -> " + timestampt());
                        }
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        producer = session.createProducer(topic);
        System.out.println("SENDING MESSAGES DURABLE: " + Thread.currentThread() + timestampt());

        return producer;
    }

    private MessageProducer nonDurable() throws NamingException, JMSException, InterruptedException {
        Topic jmsTopic = (Topic) ctx.lookup("/topic/topic.durable.test");

        consumer = session.createConsumer(jmsTopic);

        //char[] data = new char[512];
        //Arrays.fill(data, 'a');
        //String str = new String(data);
        String strMessage = gwMessage;
        TextMessage message = session.createTextMessage(strMessage);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    if(message instanceof BytesMessage) {
                        BytesMessage bytesMessage = (BytesMessage) message;
                        long len = bytesMessage.getBodyLength();
                        byte b[] = new byte[(int) len];
                        bytesMessage.readBytes(b);
                        System.out.println("MESSAGE: " + hexDump(b).length() + " -> " + timestampt());
                    }else{
                        System.out.println("MESSAGE: " + ((TextMessage) message).getText() + " -> " + timestampt());
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });


        producer = session.createProducer(jmsTopic);
        System.out.println("SENDING MESSAGES NONDURABLE: " + Thread.currentThread() + timestampt());

        return producer;


    }
}
