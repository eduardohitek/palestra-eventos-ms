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
	consumer := createConsumer("amqp://guest:guest@localhost")
	go startConsuming(consumer, "pedidos", "erp", 10, "*.pedidos", processEvent)
	forever := make(chan struct{})
	<-forever
}

func startConsuming(consumer rabbitmq.Consumer, exchangeName string, queueName string,
	concurrencyNumber int, routingKey string,
	handler func(evento models.Evento, publishKey string, exchangeName string)) {
	err := consumer.StartConsuming(
		func(d rabbitmq.Delivery) bool {
			evento, err := parseJSONToEvent(d.Body)
			if err != nil {
				log.Fatal(err)
			}
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

func processEvent(evento models.Evento, publishKey string, exchangeName string) {
	if evento.Tipo == "pedido-rejeitado" {
		color.Set(color.FgRed, color.Bold)
	}
	fmt.Printf("Registro recebido - ID: %s, origem: %s, status: %s, valor: %.2f\n", evento.Pedido.ID,
		evento.Origem, evento.Tipo, evento.Pedido.Valor)
	color.Set(color.FgYellow, color.Bold)
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

func parseJSONToEvent(eventJSON []byte) (models.Evento, error) {
	var evento models.Evento
	err := json.Unmarshal(eventJSON, &evento)
	return evento, err
}
