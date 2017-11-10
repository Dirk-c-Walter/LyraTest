package com.dirk.lyra.lyratest;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.util.Duration;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.ConfigurableConnection;
import net.jodah.lyra.config.RecoveryPolicy;
import net.jodah.lyra.config.RetryPolicy;
/**
 *
 * @author dirk
 */
public class LyraTest implements Consumer {

    public static void main(String args[]) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException, IOException, TimeoutException, InterruptedException {
        Config config = new Config()
            .withRecoveryPolicy(new RecoveryPolicy()
              .withBackoff(Duration.seconds(1), Duration.seconds(60)))
            .withRetryPolicy(new RetryPolicy()
              .withBackoff(Duration.seconds(1), Duration.seconds(60))
            );

        ConnectionOptions options = new ConnectionOptions()
                .withUri("amqp://guest:guest@96.115.237.192:5672/dirk")
                .withName("TestStream");

        ConnectionFactory confac = options.getConnectionFactory();

        ConfigurableConnection connection = Connections.create(options, config);
        Channel channel = connection.createChannel();

        AMQP.Queue.DeclareOk q = channel.queueDeclare("", false, true, true, null);
        channel.queueBind(q.getQueue(), "dirk", "");
        channel.basicConsume(q.getQueue(), true, new LyraTest());
        
        while(true) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void handleConsumeOk(String string) {
        System.err.println("Got Consume: " + string);
    }

    @Override
    public void handleCancelOk(String string) {
        System.err.println("Got Cancel: " + string);
    }

    @Override
    public void handleCancel(String string) throws IOException {
        System.err.println("Got Cancel: " + string);
    }

    @Override
    public void handleShutdownSignal(String string, ShutdownSignalException sse) {
        System.err.println("Got Shutdown: "+ string);
    }

    @Override
    public void handleRecoverOk(String string) {
        System.err.println("Got Recovery: "+ string);
    }

    @Override
    public void handleDelivery(String string, Envelope envlp, AMQP.BasicProperties bp, byte[] bytes) throws IOException {
        System.out.println("Got Message: '" + new String(bytes) + "'");
    }
}
