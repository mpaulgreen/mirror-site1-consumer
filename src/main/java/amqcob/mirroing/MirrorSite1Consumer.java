/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package amqcob.mirroing;

import org.apache.qpid.jms.JmsConnectionFactory;

import javax.jms.*;
import java.lang.IllegalStateException;

/**
 * On this example, two brokers are mirrored.
 * Everything that is happening on the first broker will be mirrored on the second, and Vice Versa.
 */
public class MirrorSite1Consumer {

    public static void main(final String[] args) throws Exception {
        ConnectionFactory cfServer0 = new JmsConnectionFactory("amqp://192.168.2.12:5772");

        Thread.sleep(1000);

        // Give it a good time before acknowledgements from the other sent over this mirror broker.

        try (Connection connection = cfServer0.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("exampleQueue");
            connection.start();
            MessageConsumer consumer = session.createConsumer(queue);
            // we will consume only half of the messages on this server
            for (int i = 50; i < 100; i++) {
                TextMessage message = (TextMessage) consumer.receive(5000);
                System.out.println("Received Message on the original server0: " + message.getText());
                if (!message.getText().equals("Message " + i)) {
                    // This is really not supposed to happen. We will throw an exception and in case it happens it needs to be investigated
                    throw new IllegalStateException("Mirror Example is not working as expected");
                }
            }
        }
    }
}
