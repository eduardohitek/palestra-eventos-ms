package main

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"math/rand"

	models "github.com/eduardohitek/event-models"
	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

func main() {
	color.Set(color.FgYellow, color.Bold)
	consumer := createConsumer("amqp://guest:guest@localhost")
	publisher := createPublisher("amqp://guest:guest@localhost")
	go startConsuming(consumer, publisher, "pedidos", "cozinha", 10, "*.criar.pedidos", processEvent)
	forever := make(chan struct{})
	<-forever
}

func createConsumer(url string) rabbitmq.Consumer {
	consumer, err := rabbitmq.NewConsumer(
		url, amqp.Config{},
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

func createPublisher(url string) rabbitmq.Publisher {
	publisher, _, err := rabbitmq.NewPublisher(
		url, amqp.Config{},
		rabbitmq.WithPublisherOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	return publisher
}

func parseJSONToEvent(eventJSON []byte) (models.Evento, error) {
	var evento models.Evento
	err := json.Unmarshal(eventJSON, &evento)
	return evento, err
}

func parseEventoToJSON(evento models.Evento) ([]byte, error) {
	eventoJSON, err := json.Marshal(evento)
	return eventoJSON, err
}

func processEvent(evento models.Evento, publishKey string, exchangeName string, publisher rabbitmq.Publisher) {
	ID := uuid.New()
	evento.Pedido.ID = ID.String()
	log.Printf("Pedido recebido: %s - SystemID: %d Sabor: %s Tamanho: %s", evento.Pedido.ID, evento.Pedido.SystemID,
		evento.Pedido.Sabor, evento.Pedido.Tamanho)
	if rand.Intn(2) == 1 {
		evento.Tipo = "pedido-confirmado"
	} else {
		evento.Tipo = "pedido-rejeitado"
	}
	log.Printf("Enviando resposta pedido: %s - %s", evento.Pedido.ID, evento.Tipo)
	publishEvent(evento, publishKey, exchangeName, publisher)

}

func startConsuming(consumer rabbitmq.Consumer, publisher rabbitmq.Publisher, exchangeName string, queueName string,
	concurrencyNumber int, routingKey string,
	handler func(evento models.Evento, publishKey string, exchangeName string, publisher rabbitmq.Publisher)) {
	err := consumer.StartConsuming(
		func(d rabbitmq.Delivery) bool {
			evento, _ := parseJSONToEvent(d.Body)
			publishKey := parseRoutingKey(d.RoutingKey)
			time.Sleep(3 * time.Second)
			handler(evento, publishKey, d.Exchange, publisher)
			return true
		},
		queueName,
		[]string{routingKey},
		rabbitmq.WithConsumeOptionsConcurrency(10),
		rabbitmq.WithConsumeOptionsQueueDurable,
		rabbitmq.WithConsumeOptionsBindingExchangeName(exchangeName),
		rabbitmq.WithConsumeOptionsBindingExchangeKind("topic"),
		rabbitmq.WithConsumeOptionsBindingExchangeDurable,
	)
	if err != nil {
		log.Fatal(err)
	}
}

func parseRoutingKey(publishKey string) string {
	return strings.Split(publishKey, ".")[0] + ".pedidos"
}

func publishEvent(evento models.Evento, publishKey string, exchangeName string, publisher rabbitmq.Publisher) {
	eventoBytes, err := parseEventoToJSON(evento)
	if err != nil {
		log.Fatal(err)
	}

	publish(publisher, eventoBytes, exchangeName, publishKey)
}

func publish(publisher rabbitmq.Publisher, evento []byte, exchangeName string, publishKey string) {
	err := publisher.Publish(
		evento,
		[]string{publishKey},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange(exchangeName),
	)
	if err != nil {
		log.Fatal(err)
	}
}
