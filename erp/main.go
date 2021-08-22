package main

import (
	"encoding/json"
	"fmt"
	"log"

	models "github.com/eduardohitek/event-models"
	"github.com/fatih/color"
	"github.com/streadway/amqp"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

func main() {
	color.Set(color.FgYellow, color.Bold)
	consumer, _ := createConsumer("amqp://guest:guest@localhost")
	go startConsuming(consumer, "pedidos", "erp", 10, "*.pedidos", processEvent)
	forever := make(chan struct{})
	<-forever
}

func createConsumer(url string) (rabbitmq.Consumer, error) {
	consumer, err := rabbitmq.NewConsumer(
		url, amqp.Config{},
		rabbitmq.WithConsumerOptionsLogging,
	)
	return consumer, err
}

func parseJSONToEvent(eventJSON []byte) (models.Evento, error) {
	var evento models.Evento
	err := json.Unmarshal(eventJSON, &evento)
	return evento, err
}

func processEvent(evento models.Evento, publishKey string, exchangeName string) {
	fmt.Printf("Registro recebido - ID: %s, origem: %s, status: %s, valor: %.2f\n", evento.Pedido.ID,
		evento.Origem, evento.Tipo, evento.Pedido.Valor)

}

func startConsuming(consumer rabbitmq.Consumer, exchangeName string, queueName string,
	concurrencyNumber int, routingKey string,
	handler func(evento models.Evento, publishKey string, exchangeName string)) {
	err := consumer.StartConsuming(
		func(d rabbitmq.Delivery) bool {
			evento, _ := parseJSONToEvent(d.Body)
			handler(evento, "", d.Exchange)
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
