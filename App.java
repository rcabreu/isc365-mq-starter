import org.apache.activemq.ActiveMQConnectionFactory;
 
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
 
public class App {
    private static String ConnectionURI =
        "ssl://b-af3f5d86-a9c1-46a8-95e9-8c719ddf45f3-1.mq.us-east-1.amazonaws.com:61617";
    private static String UserName = "isc365-mb01";
    private static String Password = "UserForTheMQISC365";
 
    public static void main(String[] args) throws Exception {
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
    }
 
    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
 
    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {
                // Configurar la fabrica de conexiones
                ActiveMQConnectionFactory connectionFactory 
                    = new ActiveMQConnectionFactory(App.ConnectionURI);
                
                connectionFactory.setUserName(App.UserName);
                connectionFactory.setPassword(App.Password);
 
                // Iniciar una sesion
                Connection connection = connectionFactory.createConnection();
                connection.start();
 
                // Crear una sesion
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
 
                // Especificar la cola
                Destination destination = session.createQueue("Colita");
 
                // Crear un producto de mensaje para la cola especificada
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
 
                // Crear un mensaje
                String text = "Hola, Mundo! " + this.hashCode();
                TextMessage message = session.createTextMessage(text);
 
                // Enviar el mensaje
                System.out.println("Enviado: " + text);
                producer.send(message);
 
                // Cerrar
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Error: " + e);
                e.printStackTrace();
            }
        }
    }
 
    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        public void run() {
            try {
 
                // Configurar la fabrica de conexiones
                ActiveMQConnectionFactory connectionFactory 
                    = new ActiveMQConnectionFactory(App.ConnectionURI);
                
                connectionFactory.setUserName(App.UserName);
                connectionFactory.setPassword(App.Password);
 
                // Iniciar una sesion
                Connection connection = connectionFactory.createConnection();
                connection.start();
                connection.setExceptionListener(this);

                // Crear una sesion
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
 
                // Especificar la cola
                Destination destination = session.createQueue("Colita");
 
                // Crear un consumidor de mensajes para la cola especificada
                MessageConsumer consumer = session.createConsumer(destination);
 
                // Bloquear esperando un mensaje
                Message message = consumer.receive(1000);

                // Mensaje recibido, imprimir
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    System.out.println("Recibi: " + text);
                } else {
                    System.out.println("Recibi: " + message);
                }
 
                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Error: " + e);
                e.printStackTrace();
            }
        }
 
        public synchronized void onException(JMSException ex) {
            System.out.println("Error en JMS.");
        }
    }
}
