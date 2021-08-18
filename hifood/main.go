package main

import (
	"encoding/json"
	"log"

	models "github.com/eduardohitek/event-models"
	"github.com/streadway/amqp"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

func main() {
	evento := models.Evento{Tipo: "criar-pedido",
		Pedido: models.Pedido{SystemID: 1, Sabor: "4 Queijos", Tamanho: "G", Valor: 38.5}}
	eventoBytes, err := json.Marshal(evento)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(string(eventoBytes))
	publicarEvento(evento)
	go consume("hifood")

	forever := make(chan struct{})
	<-forever
}

func publicarEvento(evento models.Evento) {
	eventoBytes, err := json.Marshal(evento)
	if err != nil {
		log.Fatal(err)
	}

	// log.Println(string(eventoBytes))
	publisher := createPublisher()
	publish(publisher, eventoBytes)
}

func createPublisher() rabbitmq.Publisher {
	publisher, _, err := rabbitmq.NewPublisher(
		"amqp://guest:guest@localhost", amqp.Config{},
		rabbitmq.WithPublisherOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	return publisher
}

func publish(publisher rabbitmq.Publisher, evento []byte) {
	err := publisher.Publish(
		evento,
		[]string{"hifood.criar.pedidos"},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange("pedidos"),
	)
	if err != nil {
		log.Fatal(err)
	}
}

func consume(qName string) {
	consumer, err := rabbitmq.NewConsumer(
		"amqp://guest:guest@localhost", amqp.Config{},
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	err = consumer.StartConsuming(
		func(d rabbitmq.Delivery) bool {
			log.Printf("consumed: %v", string(d.Body))
			log.Println(d.RoutingKey)
			//time.Sleep(10 * time.Second)
			//log.Println("saindo do sleep")
			//publicarEvento(models.Evento{Tipo: "teste"}, parseRoutingKey(d.RoutingKey))
			// true to ACK, false to NACK
			return true
		},
		qName,
		[]string{"hifood.*"},
		rabbitmq.WithConsumeOptionsConcurrency(10),
		rabbitmq.WithConsumeOptionsQueueDurable,
		rabbitmq.WithConsumeOptionsBindingExchangeName("pedidos"),
		rabbitmq.WithConsumeOptionsBindingExchangeKind("topic"),
		rabbitmq.WithConsumeOptionsBindingExchangeDurable,
	)
	if err != nil {
		log.Fatal(err)
	}

}
