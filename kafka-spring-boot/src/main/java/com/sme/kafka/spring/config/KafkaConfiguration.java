package com.sme.kafka.spring.config;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.context.support.GenericWebApplicationContext;

/**
 * Spring configuration.
 */
@Configuration
public class KafkaConfiguration
{
    @Value("${kafka.topics[0].name}")
    private String topic1;

    @Value("${kafka.topics[1].name}")
    private String topic2;

    private final KafkaTopicConfiguration kafkaTopicConfiguration;
    private final GenericWebApplicationContext context;

    public KafkaConfiguration(KafkaTopicConfiguration kafkaTopicConfiguration, GenericWebApplicationContext genericContext)
    {
        this.kafkaTopicConfiguration = kafkaTopicConfiguration;
        this.context = genericContext;
    }

    /**
     * Post constructor to register {@link NewTopic} beans.
     */
    @PostConstruct
    public void postConstruct()
    {
        kafkaTopicConfiguration.getTopics().forEach(topic ->
        {
            context.registerBean(topic.getName(), NewTopic.class, topic.getName(), topic.getPartitions(), topic.getReplicationFactor());
        });
    }

    /**
     * Create JSON converter.
     * 
     * @return Returns JSON message converter.
     */
    @Bean
    public RecordMessageConverter jsonConverter()
    {
        return new StringJsonMessageConverter();
    }

    /**
     * Create Concurrent Listener factory.
     * 
     * @param configurer The instance of {@link ConcurrentKafkaListenerContainerFactoryConfigurer};
     * @param kafkaConsumerFactory The Consumer factory;
     * @param template The template to perform requests to Kafka;
     * @return Returns in stance of {@link ConcurrentKafkaListenerContainerFactory}.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            KafkaTemplate<Object, Object> template)
    {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        FixedBackOff fixedBackOff = new FixedBackOff(0L, 2); // recover 2 times
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(template);
        SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, fixedBackOff);
        factory.setErrorHandler(seekToCurrentErrorHandler);
        return factory;
    }

    //CSOFF
    /*
    @Bean
    public ReplyingKafkaTemplate<?, ?, ?> replyKafkaTemplate(GenericMessageListenerContainer<Object, Object> container, ProducerFactory<Object, Object> producerFactory)
    {
        ReplyingKafkaTemplate<?, ?, ?> replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory, container);
        replyingKafkaTemplate.setSharedReplyTopic(true);
        replyingKafkaTemplate.setDefaultReplyTimeout(Duration.ofMinutes(1));
        return replyingKafkaTemplate;
    }
    @Bean
    public GenericMessageListenerContainer<Object, Object> replyContainer(ConsumerFactory<Object, Object> consumerFactory)
    {
        ContainerProperties containerProperties = new ContainerProperties(topic1, topic2);
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    }
    */
    //CSON
}
